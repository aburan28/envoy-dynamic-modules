use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use crate::config::PluginConfig;
use crate::gradient2::{self, Gradient2Params, Gradient2Result};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceUpdate {
    pub tracked_hosts: u64,
    pub overloaded_hosts: u64,
    pub adaptive_timeout_ms: u64,
    pub hosts_marked_overloaded: u64,
    pub hosts_recovered: u64,
    pub recovery_timeouts: u64,
    pub limit_recalculations: u64,
}

#[derive(Debug)]
pub struct HostState {
    pub current_limit: u32,
    pub latency_samples: Vec<u64>,
    pub min_rtt_ns: Option<u64>,
    pub windows_since_min_rtt: u32,
    pub is_probing_min_rtt: bool,
    pub probe_samples: Vec<u64>,
    pub last_seen: Instant,
    pub is_overloaded: bool,
    pub overloaded_since: Option<Instant>,
    pub last_gradient: f64,
    pub total_requests: u64,
    pub total_limited: u64,
}

impl HostState {
    pub fn new(initial_limit: u32, now: Instant) -> Self {
        Self {
            current_limit: initial_limit,
            latency_samples: Vec::with_capacity(256),
            min_rtt_ns: None,
            windows_since_min_rtt: 0,
            is_probing_min_rtt: false,
            probe_samples: Vec::new(),
            last_seen: now,
            is_overloaded: false,
            overloaded_since: None,
            last_gradient: 1.0,
            total_requests: 0,
            total_limited: 0,
        }
    }

    pub fn record_request_end(&mut self, latency_ns: u64, now: Instant) {
        self.last_seen = now;
        if self.is_probing_min_rtt {
            self.probe_samples.push(latency_ns);
        } else {
            self.latency_samples.push(latency_ns);
        }
    }

    pub fn has_enough_samples(&self, window_size: usize) -> bool {
        self.latency_samples.len() >= window_size
    }

    pub fn has_enough_probe_samples(&self, count: usize) -> bool {
        self.probe_samples.len() >= count
    }

    pub fn recalculate_limit(
        &mut self,
        config: &PluginConfig,
        now: Instant,
    ) -> Option<Gradient2Result> {
        if self.latency_samples.is_empty() {
            return None;
        }

        self.latency_samples.sort_unstable();
        let sample_rtt = gradient2::percentile(&self.latency_samples, config.sample_percentile);

        let min_rtt = match self.min_rtt_ns {
            Some(rtt) => rtt,
            None => {
                let baseline =
                    gradient2::percentile(&self.latency_samples, config.sample_percentile);
                self.min_rtt_ns = Some(baseline);
                baseline
            }
        };

        let result = gradient2::calculate_new_limit(&Gradient2Params {
            current_limit: self.current_limit,
            min_rtt_ns: min_rtt,
            sample_rtt_ns: sample_rtt,
            min_limit: config.min_concurrency_limit,
            max_limit: config.max_concurrency_limit,
            max_gradient: config.max_gradient,
            min_rtt_buffer_pct: config.min_rtt_buffer_pct,
        });

        self.current_limit = result.new_limit;
        self.last_gradient = result.gradient;
        self.latency_samples.clear();

        if result.gradient < config.overload_gradient_threshold {
            if !self.is_overloaded {
                self.overloaded_since = Some(now);
            }
            self.is_overloaded = true;
        } else if result.gradient > config.recovery_gradient_threshold {
            self.is_overloaded = false;
            self.overloaded_since = None;
        }

        self.windows_since_min_rtt += 1;

        Some(result)
    }

    pub fn start_min_rtt_probe(&mut self) {
        self.is_probing_min_rtt = true;
        self.probe_samples.clear();
    }

    pub fn finish_min_rtt_probe(&mut self, config: &PluginConfig) {
        if !self.probe_samples.is_empty() {
            self.probe_samples.sort_unstable();
            let probed_rtt = gradient2::percentile(&self.probe_samples, config.sample_percentile);
            if probed_rtt > 0 {
                self.min_rtt_ns = Some(match self.min_rtt_ns {
                    Some(existing) => ((existing as f64 * 0.8) + (probed_rtt as f64 * 0.2)) as u64,
                    None => probed_rtt,
                });
            }
        }
        self.is_probing_min_rtt = false;
        self.probe_samples.clear();
        self.windows_since_min_rtt = 0;
    }

    pub fn is_expired(&self, now: Instant, expiry: Duration) -> bool {
        now.duration_since(self.last_seen) > expiry
    }

    pub fn should_start_probe(&self, recalc_windows: u32) -> bool {
        !self.is_probing_min_rtt && self.windows_since_min_rtt >= recalc_windows
    }
}

#[derive(Debug)]
pub struct SharedState {
    pub config: PluginConfig,
    pub hosts: HashMap<String, HostState>,
    pub overloaded_hosts: HashSet<String>,
    pub adaptive_per_try_timeout_ms: u64,
    pub shed_fraction: f64,
    pub request_counter: u64,
}

impl SharedState {
    pub fn new(config: PluginConfig) -> Self {
        Self {
            config,
            hosts: HashMap::new(),
            overloaded_hosts: HashSet::new(),
            adaptive_per_try_timeout_ms: 0,
            shed_fraction: 0.0,
            request_counter: 0,
        }
    }

    pub fn get_or_create_host(&mut self, addr: &str, now: Instant) -> &mut HostState {
        let initial_limit = self.config.initial_concurrency_limit;
        self.hosts
            .entry(addr.to_string())
            .or_insert_with(|| HostState::new(initial_limit, now))
    }

    pub fn run_housekeeping(&mut self, now: Instant) -> MaintenanceUpdate {
        let mut update = MaintenanceUpdate::default();
        self.recalculate_all_limits(now, &mut update);
        self.expire_stale_hosts(now);
        self.update_overloaded_set(now, &mut update);
        self.compute_adaptive_timeout();

        update.tracked_hosts = self.hosts.len() as u64;
        update.overloaded_hosts = self.overloaded_hosts.len() as u64;
        update.adaptive_timeout_ms = self.adaptive_per_try_timeout_ms;
        update
    }

    fn recalculate_all_limits(&mut self, now: Instant, update: &mut MaintenanceUpdate) {
        let config = self.config.clone();
        for host in self.hosts.values_mut() {
            if host.is_probing_min_rtt && host.has_enough_probe_samples(config.min_rtt_probe_count)
            {
                host.finish_min_rtt_probe(&config);
            }

            if host.has_enough_samples(config.sample_window_size) {
                let was_overloaded = host.is_overloaded;
                if let Some(_result) = host.recalculate_limit(&config, now) {
                    update.limit_recalculations += 1;
                    if host.is_overloaded && !was_overloaded {
                        update.hosts_marked_overloaded += 1;
                    } else if !host.is_overloaded && was_overloaded {
                        update.hosts_recovered += 1;
                    }
                }

                if host.should_start_probe(config.min_rtt_recalc_windows) {
                    host.start_min_rtt_probe();
                }
            }
        }
    }

    fn update_overloaded_set(&mut self, now: Instant, update: &mut MaintenanceUpdate) {
        let recovery_timeout = Duration::from_secs(self.config.recovery_timeout_secs);
        self.overloaded_hosts.clear();

        for (addr, host) in self.hosts.iter_mut() {
            if host.is_overloaded {
                let overload_elapsed = host
                    .overloaded_since
                    .map(|started| now.duration_since(started))
                    .unwrap_or_default();

                if !recovery_timeout.is_zero() && overload_elapsed > recovery_timeout {
                    host.is_overloaded = false;
                    host.overloaded_since = None;
                    host.min_rtt_ns = None;
                    host.current_limit = self.config.initial_concurrency_limit;
                    host.latency_samples.clear();
                    host.probe_samples.clear();
                    host.is_probing_min_rtt = false;
                    host.windows_since_min_rtt = 0;
                    update.recovery_timeouts += 1;
                    continue;
                }

                self.overloaded_hosts.insert(addr.clone());
            }
        }

        let total = self.hosts.len();
        let overloaded = self.overloaded_hosts.len();
        self.shed_fraction = if total > 0 && overloaded > 0 {
            overloaded as f64 / total as f64
        } else {
            0.0
        };
    }

    fn compute_adaptive_timeout(&mut self) {
        if self.overloaded_hosts.is_empty() {
            self.adaptive_per_try_timeout_ms = 0;
            return;
        }

        let mut healthy_samples = Vec::new();
        for (addr, host) in &self.hosts {
            if !host.is_overloaded && !self.overloaded_hosts.contains(addr) {
                healthy_samples.extend_from_slice(&host.latency_samples);
            }
        }

        if healthy_samples.is_empty() {
            self.adaptive_per_try_timeout_ms = 200;
            return;
        }

        healthy_samples.sort_unstable();
        let p95_index = ((healthy_samples.len() as f64 * 0.95) as usize)
            .min(healthy_samples.len().saturating_sub(1));
        let healthy_p95_ns = healthy_samples[p95_index];
        self.adaptive_per_try_timeout_ms = (healthy_p95_ns / 1_000_000)
            .saturating_mul(3)
            .clamp(50, 500);
    }

    fn expire_stale_hosts(&mut self, now: Instant) {
        let expiry = Duration::from_secs(self.config.host_expiry_secs);
        self.hosts.retain(|addr, host| {
            let keep = !host.is_expired(now, expiry);
            if !keep {
                self.overloaded_hosts.remove(addr);
            }
            keep
        });
    }

    pub fn is_host_overloaded(&self, addr: &str) -> bool {
        self.overloaded_hosts.contains(addr)
    }

    pub fn has_overloaded_hosts(&self) -> bool {
        !self.overloaded_hosts.is_empty()
    }

    pub fn should_shed_request(&mut self) -> bool {
        if self.shed_fraction <= 0.0 {
            return false;
        }

        self.request_counter = self.request_counter.wrapping_add(1);
        let period = (1.0 / self.shed_fraction).round() as u64;
        if period == 0 {
            return true;
        }
        self.request_counter.is_multiple_of(period)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn deterministic_shedding_matches_fraction() {
        let mut state = SharedState::new(PluginConfig::default());
        state.shed_fraction = 0.2;

        let shed_positions: Vec<u64> = (1..=10).filter(|_| state.should_shed_request()).collect();
        assert_eq!(shed_positions.len(), 2);
        assert_eq!(state.request_counter, 10);
    }

    #[test]
    fn overloaded_host_resets_after_recovery_timeout() {
        let mut state = SharedState::new(PluginConfig {
            recovery_timeout_secs: 1,
            ..PluginConfig::default()
        });
        let now = Instant::now();
        let mut host = HostState::new(100, now - Duration::from_secs(5));
        host.is_overloaded = true;
        host.overloaded_since = Some(now - Duration::from_secs(3));
        host.min_rtt_ns = Some(5_000_000);
        host.current_limit = 7;
        state.hosts.insert("10.0.0.1:8080".into(), host);

        let update = state.run_housekeeping(now);
        let host = state.hosts.get("10.0.0.1:8080").unwrap();

        assert!(!host.is_overloaded);
        assert_eq!(host.current_limit, 100);
        assert_eq!(update.recovery_timeouts, 1);
    }

    #[test]
    fn adaptive_timeout_uses_healthy_hosts() {
        let mut state = SharedState::new(PluginConfig::default());
        let now = Instant::now();

        let mut overloaded = HostState::new(100, now);
        overloaded.is_overloaded = true;
        overloaded.overloaded_since = Some(now);
        state.hosts.insert("10.0.0.1:8080".into(), overloaded);

        let mut healthy = HostState::new(100, now);
        healthy.latency_samples = vec![10_000_000, 20_000_000, 30_000_000];
        state.hosts.insert("10.0.0.2:8080".into(), healthy);

        state.run_housekeeping(now);
        assert_eq!(state.adaptive_per_try_timeout_ms, 90);
    }
}
