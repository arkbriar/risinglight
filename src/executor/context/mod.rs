use std::intrinsics;

use tokio_util::sync::CancellationToken;

pub struct Context {
    #[allow(dead_code)]
    token: CancellationToken,
}

impl Context {
    pub fn new() -> Self {
        Self {
            token: Default::default(),
        }
    }
}

impl Context {
    pub fn cancel(&self) {
        self.token.cancel()
    }

    pub fn child_cancellation_token(&self) -> CancellationToken {
        self.token.child_token()
    }

    #[inline(always)]
    #[allow(unused_unsafe)]
    pub fn is_cancelled(&self) -> bool {
        unsafe { intrinsics::unlikely(self.token.is_cancelled()) }
    }

    pub async fn cancelled(&self) {
        self.token.cancelled().await
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}
