mod config;
mod gradient2;
mod http_filter;
mod metrics;
mod state;

use envoy_proxy_dynamic_modules_rust_sdk::*;

use crate::http_filter::AdaptiveConcurrencyFilterConfig;

declare_init_functions!(init, new_http_filter_config_fn);

fn init() -> bool {
    true
}

fn new_http_filter_config_fn<EC: EnvoyHttpFilterConfig, EHF: EnvoyHttpFilter>(
    envoy_filter_config: &mut EC,
    filter_name: &str,
    filter_config: &[u8],
) -> Option<Box<dyn HttpFilterConfig<EHF>>> {
    if filter_name != "adaptive_concurrency" {
        envoy_log_error!("adaptive_concurrency: unknown filter name: {}", filter_name);
        return None;
    }

    match AdaptiveConcurrencyFilterConfig::new(filter_config, envoy_filter_config) {
        Ok(config) => Some(Box::new(config)),
        Err(err) => {
            envoy_log_error!("adaptive_concurrency: config error: {}", err);
            None
        }
    }
}
