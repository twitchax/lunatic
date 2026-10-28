use std::{fmt::Display, sync::Arc};

use axum::{
    extract::{FromRequest, FromRequestParts, Path, Request},
    http::{self, request::Parts, HeaderMap},
    response::{IntoResponse, Response},
    Extension, Json,
};
use http::header;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::json;

use crate::server::ControlServer;

pub type ApiResponse<D> = Result<Json<D>, ApiError>;

pub fn ok<D: Serialize>(data: D) -> ApiResponse<D> {
    Ok(Json(data))
}

#[derive(Debug)]
pub enum ApiError {
    Internal,
    NotAuthenticated,
    NotAuthorized,
    // The node's lease ran out, so it's no longer part of the cluster.
    NodeExpired,
    InvalidData(String),
    InvalidPathArg(String),
    InvalidQueryArg(String),
    Custom {
        code: &'static str,
        message: Option<String>,
    },
}

impl ApiError {
    pub fn code(&self) -> &str {
        match self {
            ApiError::Internal => "internal",
            ApiError::NotAuthenticated => "unauthenticated",
            ApiError::NotAuthorized => "unauthorized",
            ApiError::NodeExpired => "node_expired",
            ApiError::InvalidData(_) => "invalid_data",
            ApiError::InvalidPathArg(_) => "invalid_path_arg",
            ApiError::InvalidQueryArg(_) => "invalid_query_arg",
            ApiError::Custom { code, .. } => code,
        }
    }

    pub fn message(&self) -> String {
        match self {
            ApiError::Internal => "".into(),
            ApiError::NotAuthenticated => "Not authenticated".into(),
            ApiError::NotAuthorized => "Not authorized".into(),
            ApiError::NodeExpired => "The node's lease expired".into(),
            ApiError::InvalidData(msg) => msg.clone(),
            ApiError::InvalidPathArg(msg) => msg.clone(),
            ApiError::InvalidQueryArg(msg) => msg.clone(),
            ApiError::Custom { message, .. } => message.clone().unwrap_or_else(|| "".into()),
        }
    }

    #[allow(dead_code)]
    pub fn log_internal(msg: &str, e: impl std::fmt::Debug) -> Self {
        log::error!("{}: {:?}", msg, e);
        Self::Internal
    }

    pub fn custom(code: &'static str, message: String) -> Self {
        Self::Custom {
            code,
            message: Some(message),
        }
    }

    pub fn custom_code(code: &'static str) -> Self {
        Self::Custom {
            code,
            message: None,
        }
    }
}

impl Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Error ")?;
        f.write_str(self.code())?;
        let msg = self.message();
        if !msg.is_empty() {
            f.write_str(": ")?;
            f.write_str(&msg)?;
        }
        Ok(())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        use http::StatusCode as S;
        use ApiError::*;

        let body = Json(json!({
            "message": self.message(),
            "code": self.code(),
        }));

        let status = match self {
            Self::Internal => S::INTERNAL_SERVER_ERROR,
            Self::NotAuthenticated => S::UNAUTHORIZED,
            Self::NotAuthorized => S::FORBIDDEN,
            Self::NodeExpired => S::GONE,
            InvalidData(_) | InvalidPathArg(_) | InvalidQueryArg(_) | Custom { .. } => {
                S::BAD_REQUEST
            }
        };

        (status, body).into_response()
    }
}

pub struct JsonExtractor<T>(pub T);

impl<S, T> FromRequest<S> for JsonExtractor<T>
where
    axum::Json<T>: FromRequest<S, Rejection = axum::extract::rejection::JsonRejection>,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<JsonExtractor<T>, Self::Rejection> {
        match Json::from_request(req, state).await {
            Ok(Json(value)) => Ok(JsonExtractor(value)),
            Err(e) => Err(ApiError::InvalidData(e.to_string())),
        }
    }
}

pub struct PathExtractor<T>(pub T);

impl<S, T> FromRequestParts<S> for PathExtractor<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Send,
{
    type Rejection = ApiError;

    async fn from_request_parts(
        req: &mut Parts,
        state: &S,
    ) -> Result<PathExtractor<T>, Self::Rejection> {
        match Path::from_request_parts(req, state).await {
            Ok(Path(value)) => Ok(PathExtractor(value)),
            Err(e) => Err(ApiError::InvalidPathArg(e.to_string())),
        }
    }
}

pub struct HostExtractor(pub String);

impl<S> FromRequestParts<S> for HostExtractor
where
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(
        req: &mut Parts,
        _state: &S,
    ) -> Result<HostExtractor, Self::Rejection> {
        match host(req) {
            Some(host) => Ok(HostExtractor(host.to_owned())),
            None => Err(ApiError::Custom {
                code: "no_host",
                message: Some("No host found in request".into()),
            }),
        }
    }
}

// Resolves the host the way axum's former `Host` extractor did, which axum no longer provides.
fn host(req: &Parts) -> Option<&str> {
    if let Some(host) = forwarded_host(&req.headers) {
        return Some(host);
    }

    if let Some(host) = req
        .headers
        .get("X-Forwarded-Host")
        .and_then(|host| host.to_str().ok())
    {
        return Some(host);
    }

    if let Some(host) = req
        .headers
        .get(header::HOST)
        .and_then(|host| host.to_str().ok())
    {
        return Some(host);
    }

    req.uri.host()
}

fn forwarded_host(headers: &HeaderMap) -> Option<&str> {
    // If there are multiple `Forwarded` headers, `HeaderMap::get` returns the first one.
    let forwarded_values = headers.get(header::FORWARDED)?.to_str().ok()?;

    // Get the first set of values.
    let first_value = forwarded_values.split(',').next()?;

    // Find the value of the `host` field.
    first_value.split(';').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        key.trim()
            .eq_ignore_ascii_case("host")
            .then(|| value.trim().trim_matches('"'))
    })
}

#[derive(Debug)]
pub struct NodeAuth {
    pub registration_id: i64,
    pub node_name: uuid::Uuid,
}

impl<S> FromRequestParts<S> for NodeAuth
where
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(req: &mut Parts, state: &S) -> Result<NodeAuth, Self::Rejection> {
        let headers = req.headers.clone();
        let auth_header = headers
            .get(header::AUTHORIZATION)
            .ok_or_else(|| {
                ApiError::custom("no_auth_header", "Missing node authorization header".into())
            })?
            .to_str()
            .map_err(|_| {
                ApiError::custom(
                    "invalid_auth_header",
                    "Invalid authorization header value".into(),
                )
            })?;

        let token = auth_header
            .strip_prefix("Bearer ")
            .to_owned()
            .ok_or_else(|| {
                ApiError::custom(
                    "invalid_auth_token",
                    "Header value doesn't start with Bearer".into(),
                )
            })?;

        let node_name = headers
            .get("x-lunatic-node-name")
            .ok_or_else(|| {
                ApiError::custom(
                    "no_lunatic_node_name_header",
                    "Missing x-lunatic-node-name header".into(),
                )
            })?
            .to_str()
            .map_err(|_| {
                ApiError::custom(
                    "invalid_lunatic_node_name_header",
                    "Invalid x-lunatic-node-name header value".into(),
                )
            })?;

        let node_name: uuid::Uuid = node_name.parse().map_err(|_| {
            ApiError::custom(
                "invalid_lunatic_node_name_header",
                format!("Invalid x-lunatic-node-name header: {node_name} not a valid UUID"),
            )
        })?;

        let cs: Extension<Arc<ControlServer>> = Extension::from_request_parts(req, state)
            .await
            .map_err(|e| ApiError::log_internal("Error getting cs in registration auth", e))?;

        let (registration_id, reg) = cs
            .registrations
            .iter()
            .find(|r| r.node_name == node_name && r.authentication_token == token)
            .map(|r| (*r.key(), r.value().clone()))
            .ok_or(ApiError::NotAuthenticated)?;
        // Every authenticated request renews the node's lease.
        cs.renew_lease(registration_id)
            .map_err(|_| ApiError::NodeExpired)?;
        let node_auth = NodeAuth {
            registration_id: registration_id as i64,
            node_name: reg.node_name,
        };

        Ok(node_auth)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(uri: &str, headers: &[(&str, &str)]) -> Parts {
        let mut request = http::Request::builder().uri(uri);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        request.body(()).unwrap().into_parts().0
    }

    #[test]
    fn host_prefers_forwarded_headers() {
        let all = [
            (
                "Forwarded",
                "for=1.2.3.4; host=\"forwarded:1\";proto=http, host=second",
            ),
            ("X-Forwarded-Host", "x-forwarded:2"),
            ("Host", "host:3"),
        ];
        assert_eq!(host(&parts("http://uri:4/", &all)), Some("forwarded:1"));
        assert_eq!(
            host(&parts("http://uri:4/", &all[1..])),
            Some("x-forwarded:2")
        );
        assert_eq!(host(&parts("http://uri:4/", &all[2..])), Some("host:3"));
        assert_eq!(host(&parts("http://uri:4/", &[])), Some("uri"));
        assert_eq!(host(&parts("/", &[])), None);
    }
}
