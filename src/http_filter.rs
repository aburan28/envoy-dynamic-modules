use std::sync::{Arc, Mutex};
use std::time::Instant;

use envoy_proxy_dynamic_modules_rust_sdk::*;

use crate::config::PluginConfig;
use crate::metrics::PluginMetrics;
use crate::state::{MaintenanceUpdate, SharedState};

pub struct AdaptiveConcurrencyFilterConfig {
    shared: Arc<Mutex<SharedState>>,
    metrics: PluginMetrics,
}

impl AdaptiveConcurrencyFilterConfig {
    pub fn new<EC: EnvoyHttpFilterConfig>(
        config_bytes: &[u8],
        envoy_filter_config: &mut EC,
    ) -> Result<Self, String> {
        let config = PluginConfig::from_json(config_bytes)?;
        let metrics = PluginMetrics::new(envoy_filter_config);
        Ok(Self {
            shared: Arc::new(Mutex::new(SharedState::new(config))),
            metrics,
        })
    }
}

impl<EHF: EnvoyHttpFilter> HttpFilterConfig<EHF> for AdaptiveConcurrencyFilterConfig {
    fn new_http_filter(&self, _envoy: &mut EHF) -> Box<dyn HttpFilter<EHF>> {
        Box::new(AdaptiveConcurrencyFilter {
            shared: Arc::clone(&self.shared),
            metrics: self.metrics.clone(),
            request_start: None,
            upstream_address: None,
            was_shed: false,
            response_recorded: false,
        })
    }
}

pub struct AdaptiveConcurrencyFilter {
    shared: Arc<Mutex<SharedState>>,
    metrics: PluginMetrics,
    request_start: Option<Instant>,
    upstream_address: Option<String>,
    was_shed: bool,
    response_recorded: bool,
}

impl AdaptiveConcurrencyFilter {
    fn record_housekeeping<EHF: EnvoyHttpFilter>(
        &self,
        envoy_filter: &EHF,
        update: MaintenanceUpdate,
        last_gradient: Option<f64>,
    ) {
        self.metrics
            .add_hosts_marked_overloaded(envoy_filter, update.hosts_marked_overloaded);
        self.metrics
            .add_hosts_recovered(envoy_filter, update.hosts_recovered);
        self.metrics
            .add_recovery_timeouts(envoy_filter, update.recovery_timeouts);
        self.metrics
            .add_limit_recalculations(envoy_filter, update.limit_recalculations);
        self.metrics
            .set_tracked_hosts(envoy_filter, update.tracked_hosts);
        self.metrics
            .set_overloaded_hosts(envoy_filter, update.overloaded_hosts);
        self.metrics
            .set_adaptive_timeout_ms(envoy_filter, update.adaptive_timeout_ms);

        if let Some(gradient) = last_gradient {
            self.metrics.record_gradient(envoy_filter, gradient);
        }
    }

    fn get_upstream_address<EHF: EnvoyHttpFilter>(&self, envoy_filter: &EHF) -> Option<String> {
        let address = envoy_filter
            .get_attribute_string(abi::envoy_dynamic_module_type_attribute_id::UpstreamAddress)
            .map(|buffer| String::from_utf8_lossy(buffer.as_slice()).into_owned())?;

        match envoy_filter
            .get_attribute_int(abi::envoy_dynamic_module_type_attribute_id::UpstreamPort)
        {
            Some(port) if port > 0 => Some(format!("{address}:{port}")),
            _ => Some(address),
        }
    }
}

impl<EHF: EnvoyHttpFilter> HttpFilter<EHF> for AdaptiveConcurrencyFilter {
    fn on_request_headers(
        &mut self,
        envoy_filter: &mut EHF,
        _end_of_stream: bool,
    ) -> abi::envoy_dynamic_module_type_on_http_filter_request_headers_status {
        let now = Instant::now();
        self.request_start = Some(now);
        self.was_shed = false;
        self.response_recorded = false;
        self.upstream_address = None;

        let mut shared = self.shared.lock().unwrap();
        let update = shared.run_housekeeping(now);
        let adaptive_timeout_ms = shared.adaptive_per_try_timeout_ms;
        let has_overloaded_hosts = shared.has_overloaded_hosts();
        let dry_run = shared.config.dry_run;

        self.record_housekeeping(envoy_filter, update, None);

        if !has_overloaded_hosts || dry_run {
            if has_overloaded_hosts && dry_run {
                envoy_log_warn!(
                    "adaptive_concurrency: [DRY RUN] would shed {:.0}% of requests ({} overloaded hosts, adaptive_timeout={}ms)",
                    shared.shed_fraction * 100.0,
                    shared.overloaded_hosts.len(),
                    adaptive_timeout_ms,
                );
            }
            return abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue;
        }

        if shared.should_shed_request() {
            let response_code = shared.config.overload_response_code;
            let shed_pct = shared.shed_fraction * 100.0;
            let overloaded_count = shared.overloaded_hosts.len();
            self.metrics.inc_requests_shed(envoy_filter);
            drop(shared);

            let body = format!(
                "adaptive concurrency: request shed ({shed_pct:.0}% of cluster overloaded, {overloaded_count} hosts)\n"
            );
            let headers = &[
                ("x-envoy-overloaded", b"true" as &[u8]),
                ("x-adaptive-concurrency-shed", b"true" as &[u8]),
                ("content-type", b"text/plain" as &[u8]),
            ];
            envoy_filter.send_response(
                response_code,
                headers,
                Some(body.as_bytes()),
                Some("adaptive_concurrency_shed"),
            );
            self.was_shed = true;
            envoy_log_info!(
                "adaptive_concurrency: shed request (shed_fraction={:.2}, overloaded={})",
                shed_pct / 100.0,
                overloaded_count
            );
            return abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration;
        }

        let _ = envoy_filter.set_request_header("x-adaptive-concurrency-active", b"true");
        if adaptive_timeout_ms > 0 {
            let timeout_ms = adaptive_timeout_ms.to_string();
            let _ = envoy_filter.set_request_header(
                "x-envoy-upstream-rq-per-try-timeout-ms",
                timeout_ms.as_bytes(),
            );
        }

        abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
    }

    fn on_response_headers(
        &mut self,
        envoy_filter: &mut EHF,
        _end_of_stream: bool,
    ) -> abi::envoy_dynamic_module_type_on_http_filter_response_headers_status {
        if self.was_shed || self.response_recorded {
            return abi::envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue;
        }

        let Some(request_start) = self.request_start else {
            return abi::envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue;
        };

        let Some(addr) = self.get_upstream_address(envoy_filter) else {
            return abi::envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue;
        };

        let now = Instant::now();
        let latency_ns = request_start.elapsed().as_nanos() as u64;
        let latency_ms = latency_ns / 1_000_000;
        self.upstream_address = Some(addr.clone());
        self.response_recorded = true;

        let (update, is_overloaded, last_gradient) = {
            let mut shared = self.shared.lock().unwrap();
            let host = shared.get_or_create_host(&addr, now);
            host.record_request_end(latency_ns, now);
            host.total_requests += 1;
            let last_gradient = host.last_gradient;
            let update = shared.run_housekeeping(now);
            let is_overloaded = shared.is_host_overloaded(&addr);
            if is_overloaded {
                if let Some(host) = shared.hosts.get_mut(&addr) {
                    host.total_limited += 1;
                }
            }
            (update, is_overloaded, Some(last_gradient))
        };

        self.metrics.inc_requests_total(envoy_filter);
        self.metrics
            .record_request_latency_ms(envoy_filter, latency_ms);
        self.record_housekeeping(envoy_filter, update, last_gradient);

        if is_overloaded {
            let _ = envoy_filter.set_response_header("x-adaptive-concurrency-limited", b"true");
            let _ = envoy_filter.set_response_header("x-overloaded-host", addr.as_bytes());
            self.metrics.inc_overloaded_responses(envoy_filter);
        }

        abi::envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use envoy_proxy_dynamic_modules_rust_sdk::{abi, EnvoyBuffer, HttpFilter, MockEnvoyHttpFilter};

    use super::*;
    use crate::state::{HostState, SharedState};

    fn test_filter(state: SharedState) -> AdaptiveConcurrencyFilter {
        AdaptiveConcurrencyFilter {
            shared: Arc::new(Mutex::new(state)),
            metrics: PluginMetrics::default(),
            request_start: None,
            upstream_address: None,
            was_shed: false,
            response_recorded: false,
        }
    }

    #[test]
    fn request_is_shed_when_fraction_is_one() {
        let now = Instant::now();
        let mut state = SharedState::new(PluginConfig::default());
        let mut host = HostState::new(100, now);
        host.is_overloaded = true;
        host.overloaded_since = Some(now);
        state.hosts.insert("10.0.0.1:8080".into(), host);
        state.overloaded_hosts.insert("10.0.0.1:8080".into());
        state.shed_fraction = 1.0;

        let mut filter = test_filter(state);
        let mut envoy = MockEnvoyHttpFilter::default();
        envoy.expect_send_response().times(1).return_const(());

        let status = filter.on_request_headers(&mut envoy, false);
        assert_eq!(
            status,
            abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration
        );
    }

    #[test]
    fn active_requests_get_timeout_header_when_overloaded_hosts_exist() {
        let now = Instant::now();
        let mut state = SharedState::new(PluginConfig::default());
        let mut overloaded = HostState::new(100, now);
        overloaded.is_overloaded = true;
        overloaded.overloaded_since = Some(now);
        state.hosts.insert("10.0.0.1:8080".into(), overloaded);

        let mut healthy = HostState::new(100, now);
        healthy.latency_samples = vec![10_000_000, 20_000_000, 30_000_000];
        state.hosts.insert("10.0.0.2:8080".into(), healthy);

        let mut filter = test_filter(state);
        let mut envoy = MockEnvoyHttpFilter::default();
        envoy
            .expect_set_request_header()
            .withf(|key, value| {
                key == "x-adaptive-concurrency-active" && value == b"true".as_slice()
            })
            .times(1)
            .return_const(true);
        envoy
            .expect_set_request_header()
            .withf(|key, value| {
                key == "x-envoy-upstream-rq-per-try-timeout-ms" && value == b"90".as_slice()
            })
            .times(1)
            .return_const(true);

        let status = filter.on_request_headers(&mut envoy, false);
        assert_eq!(
            status,
            abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
        );
    }

    #[test]
    fn response_records_latency_for_upstream_host() {
        let mut filter = test_filter(SharedState::new(PluginConfig::default()));
        filter.request_start = Some(Instant::now() - Duration::from_millis(5));

        let mut envoy = MockEnvoyHttpFilter::default();
        envoy
            .expect_get_attribute_string()
            .withf(|attribute| {
                *attribute == abi::envoy_dynamic_module_type_attribute_id::UpstreamAddress
            })
            .times(1)
            .returning(|_| Some(EnvoyBuffer::new("10.0.0.5")));
        envoy
            .expect_get_attribute_int()
            .withf(|attribute| {
                *attribute == abi::envoy_dynamic_module_type_attribute_id::UpstreamPort
            })
            .times(1)
            .return_const(Some(8080));

        let status = filter.on_response_headers(&mut envoy, false);
        assert_eq!(
            status,
            abi::envoy_dynamic_module_type_on_http_filter_response_headers_status::Continue
        );

        let shared = filter.shared.lock().unwrap();
        let host = shared.hosts.get("10.0.0.5:8080").unwrap();
        assert_eq!(host.total_requests, 1);
        assert_eq!(host.latency_samples.len(), 1);
    }
}
