use envoy_proxy_dynamic_modules_rust_sdk::*;

#[derive(Debug, Clone, Default)]
pub struct PluginMetrics {
    pub requests_total: Option<EnvoyCounterId>,
    pub requests_shed: Option<EnvoyCounterId>,
    pub overloaded_responses: Option<EnvoyCounterId>,
    pub hosts_marked_overloaded: Option<EnvoyCounterId>,
    pub hosts_recovered: Option<EnvoyCounterId>,
    pub recovery_timeouts: Option<EnvoyCounterId>,
    pub limit_recalculations: Option<EnvoyCounterId>,
    pub tracked_hosts: Option<EnvoyGaugeId>,
    pub overloaded_hosts: Option<EnvoyGaugeId>,
    pub adaptive_timeout_ms: Option<EnvoyGaugeId>,
    pub request_latency_ms: Option<EnvoyHistogramId>,
    pub gradient: Option<EnvoyHistogramId>,
}

impl PluginMetrics {
    pub fn new<EC: EnvoyHttpFilterConfig>(envoy_filter_config: &mut EC) -> Self {
        Self {
            requests_total: envoy_filter_config
                .define_counter("adaptive_concurrency_requests_total")
                .ok(),
            requests_shed: envoy_filter_config
                .define_counter("adaptive_concurrency_requests_shed")
                .ok(),
            overloaded_responses: envoy_filter_config
                .define_counter("adaptive_concurrency_overloaded_responses")
                .ok(),
            hosts_marked_overloaded: envoy_filter_config
                .define_counter("adaptive_concurrency_hosts_marked_overloaded")
                .ok(),
            hosts_recovered: envoy_filter_config
                .define_counter("adaptive_concurrency_hosts_recovered")
                .ok(),
            recovery_timeouts: envoy_filter_config
                .define_counter("adaptive_concurrency_recovery_timeouts")
                .ok(),
            limit_recalculations: envoy_filter_config
                .define_counter("adaptive_concurrency_limit_recalculations")
                .ok(),
            tracked_hosts: envoy_filter_config
                .define_gauge("adaptive_concurrency_tracked_hosts")
                .ok(),
            overloaded_hosts: envoy_filter_config
                .define_gauge("adaptive_concurrency_overloaded_hosts")
                .ok(),
            adaptive_timeout_ms: envoy_filter_config
                .define_gauge("adaptive_concurrency_adaptive_timeout_ms")
                .ok(),
            request_latency_ms: envoy_filter_config
                .define_histogram("adaptive_concurrency_request_latency_ms")
                .ok(),
            gradient: envoy_filter_config
                .define_histogram("adaptive_concurrency_gradient")
                .ok(),
        }
    }

    pub fn inc_requests_total<EHF: EnvoyHttpFilter>(&self, envoy_filter: &EHF) {
        self.increment_counter(envoy_filter, self.requests_total, 1);
    }

    pub fn inc_requests_shed<EHF: EnvoyHttpFilter>(&self, envoy_filter: &EHF) {
        self.increment_counter(envoy_filter, self.requests_shed, 1);
    }

    pub fn inc_overloaded_responses<EHF: EnvoyHttpFilter>(&self, envoy_filter: &EHF) {
        self.increment_counter(envoy_filter, self.overloaded_responses, 1);
    }

    pub fn add_hosts_marked_overloaded<EHF: EnvoyHttpFilter>(
        &self,
        envoy_filter: &EHF,
        value: u64,
    ) {
        self.increment_counter(envoy_filter, self.hosts_marked_overloaded, value);
    }

    pub fn add_hosts_recovered<EHF: EnvoyHttpFilter>(&self, envoy_filter: &EHF, value: u64) {
        self.increment_counter(envoy_filter, self.hosts_recovered, value);
    }

    pub fn add_recovery_timeouts<EHF: EnvoyHttpFilter>(&self, envoy_filter: &EHF, value: u64) {
        self.increment_counter(envoy_filter, self.recovery_timeouts, value);
    }

    pub fn add_limit_recalculations<EHF: EnvoyHttpFilter>(&self, envoy_filter: &EHF, value: u64) {
        self.increment_counter(envoy_filter, self.limit_recalculations, value);
    }

    pub fn set_tracked_hosts<EHF: EnvoyHttpFilter>(&self, envoy_filter: &EHF, value: u64) {
        self.set_gauge(envoy_filter, self.tracked_hosts, value);
    }

    pub fn set_overloaded_hosts<EHF: EnvoyHttpFilter>(&self, envoy_filter: &EHF, value: u64) {
        self.set_gauge(envoy_filter, self.overloaded_hosts, value);
    }

    pub fn set_adaptive_timeout_ms<EHF: EnvoyHttpFilter>(&self, envoy_filter: &EHF, value: u64) {
        self.set_gauge(envoy_filter, self.adaptive_timeout_ms, value);
    }

    pub fn record_request_latency_ms<EHF: EnvoyHttpFilter>(&self, envoy_filter: &EHF, value: u64) {
        if let Some(id) = self.request_latency_ms {
            let _ = envoy_filter.record_histogram_value(id, value);
        }
    }

    pub fn record_gradient<EHF: EnvoyHttpFilter>(&self, envoy_filter: &EHF, gradient: f64) {
        if let Some(id) = self.gradient {
            let scaled = (gradient * 1000.0) as u64;
            let _ = envoy_filter.record_histogram_value(id, scaled);
        }
    }

    fn increment_counter<EHF: EnvoyHttpFilter>(
        &self,
        envoy_filter: &EHF,
        id: Option<EnvoyCounterId>,
        value: u64,
    ) {
        if value == 0 {
            return;
        }
        if let Some(id) = id {
            let _ = envoy_filter.increment_counter(id, value);
        }
    }

    fn set_gauge<EHF: EnvoyHttpFilter>(
        &self,
        envoy_filter: &EHF,
        id: Option<EnvoyGaugeId>,
        value: u64,
    ) {
        if let Some(id) = id {
            let _ = envoy_filter.set_gauge(id, value);
        }
    }
}
