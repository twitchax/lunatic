use std::path::Path;

use anyhow::Result;
use rcgen::*;

pub static TEST_ROOT_CERT: &str = r#"""
-----BEGIN CERTIFICATE-----
MIIBnDCCAUGgAwIBAgIIR5Hk+O5RdOgwCgYIKoZIzj0EAwIwKTEQMA4GA1UEAwwH
Um9vdCBDQTEVMBMGA1UECgwMTHVuYXRpYyBJbmMuMCAXDTc1MDEwMTAwMDAwMFoY
DzQwOTYwMTAxMDAwMDAwWjApMRAwDgYDVQQDDAdSb290IENBMRUwEwYDVQQKDAxM
dW5hdGljIEluYy4wWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAARlVNxYAwsmmFNc
2EMBbZZVwL8GBtnnu8IROdDd68ixc0VBjfrV0zAM344lKJcs9slsMTEofoYvMCpI
BhnSGyAFo1EwTzAdBgNVHREEFjAUghJyb290Lmx1bmF0aWMuY2xvdWQwHQYDVR0O
BBYEFOh0Ue745JFH76xErjqkW2/SbHhAMA8GA1UdEwEB/wQFMAMBAf8wCgYIKoZI
zj0EAwIDSQAwRgIhAJKPv4XUZ9ej+CVgsJ+9x/CmJEcnebyWh2KntJri97nxAiEA
/KvaQE6GtYZPGFv/WYM3YEmTQ7hoOvaaAuvD27cHkaw=
-----END CERTIFICATE-----
"""#;

pub static CTRL_SERVER_NAME: &str = "ctrl.lunatic.cloud";

static TEST_ROOT_KEYS: &str = r#"""
-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg9ferf0du4h975Jhu
boMyGfdI+xwp7ewOulGvpTcvdpehRANCAARlVNxYAwsmmFNc2EMBbZZVwL8GBtnn
u8IROdDd68ixc0VBjfrV0zAM344lKJcs9slsMTEofoYvMCpIBhnSGyAF
-----END PRIVATE KEY-----"""#;

/// The test root CA as an issuer of certificates.
pub fn test_root_cert() -> Result<Issuer<'static, KeyPair>> {
    root_cert_from_pem(TEST_ROOT_CERT, TEST_ROOT_KEYS)
}

/// The test root CA's certificate and private key, both PEM encoded.
pub fn test_root_cert_pem() -> (String, String) {
    (TEST_ROOT_CERT.to_string(), TEST_ROOT_KEYS.to_string())
}

pub fn root_cert(ca_cert: &str, ca_keys: &str) -> Result<Issuer<'static, KeyPair>> {
    let ca_cert_pem = std::fs::read(Path::new(ca_cert))?;
    let ca_keys_pem = std::fs::read(Path::new(ca_keys))?;
    root_cert_from_pem(
        std::str::from_utf8(&ca_cert_pem)?,
        std::str::from_utf8(&ca_keys_pem)?,
    )
}

/// A CA, given its PEM encoded certificate and private key, as an issuer of certificates.
pub fn root_cert_from_pem(ca_cert: &str, ca_keys: &str) -> Result<Issuer<'static, KeyPair>> {
    let key_pair = KeyPair::from_pem(ca_keys)?;
    Ok(Issuer::from_ca_cert_pem(ca_cert, key_pair)?)
}

fn ctrl_cert_params() -> Result<CertificateParams> {
    let mut ctrl_params = CertificateParams::new(vec![CTRL_SERVER_NAME.into()])?;
    ctrl_params
        .distinguished_name
        .push(DnType::OrganizationName, "Lunatic Inc.");
    ctrl_params
        .distinguished_name
        .push(DnType::CommonName, "Control CA");
    Ok(ctrl_params)
}

pub fn default_server_certificates(
    root_cert: &Issuer<'_, impl SigningKey>,
) -> Result<(String, String)> {
    let ctrl_key = KeyPair::generate()?;
    let ctrl_cert = ctrl_cert_params()?.signed_by(&ctrl_key, root_cert)?;
    Ok((ctrl_cert.pem(), ctrl_key.serialize_pem()))
}
