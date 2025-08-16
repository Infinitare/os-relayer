use solana_sdk::pubkey::Pubkey;
use crate::relayer::auth::auth_service::ValidatorAuther;

pub mod auth_service;
pub mod auth_challenges;
pub mod auth_interceptor;

#[derive(Default, Clone)]
pub struct ValidatorAutherImpl {}

impl ValidatorAuther for ValidatorAutherImpl {
    fn is_authorized(&self, _: &Pubkey) -> bool {
        true
    }
}
