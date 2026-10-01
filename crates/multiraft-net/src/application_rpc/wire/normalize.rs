//! Retain Tonic0.12 outer decoder status compatibility.
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tonic::Status;
use tower::{Layer, Service};
#[derive(Clone, Copy)]
pub(super) struct NormalizeOutOfRangeLayer;
#[derive(Clone)]
pub(super) struct NormalizeOutOfRangeService<S> {
    inner: S,
}
impl<S> Layer<S> for NormalizeOutOfRangeLayer {
    type Service = NormalizeOutOfRangeService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        NormalizeOutOfRangeService { inner }
    }
}

impl<S, RequestBody, ResponseBody> Service<tonic::codegen::http::Request<RequestBody>>
    for NormalizeOutOfRangeService<S>
where
    S: Service<
        tonic::codegen::http::Request<RequestBody>,
        Response = tonic::codegen::http::Response<ResponseBody>,
    >,
    S::Future: Send + 'static,
    ResponseBody: Send + 'static,
{
    type Response = tonic::codegen::http::Response<ResponseBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: tonic::codegen::http::Request<RequestBody>) -> Self::Future {
        let response = self.inner.call(request);
        Box::pin(async move {
            let mut response = response.await?;
            normalize_decoder_status(response.headers_mut());
            Ok(response)
        })
    }
}

fn normalize_decoder_status(headers: &mut tonic::codegen::http::HeaderMap) {
    let is_out_of_range = headers
        .get(Status::GRPC_STATUS)
        .is_some_and(|value| value.as_bytes() == b"11");
    if is_out_of_range {
        headers.insert(
            Status::GRPC_STATUS,
            tonic::codegen::http::HeaderValue::from_static("8"),
        );
    }
}
