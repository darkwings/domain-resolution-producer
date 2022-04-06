package com.nttdata.bulk;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import lombok.val;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Optional.ofNullable;
import static org.apache.kafka.clients.CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_CIPHER_SUITES_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_KEY_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_TYPE_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEY_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_PROVIDER_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG;

@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor()
@Builder
@Getter
@ToString
public class EncryptionConfig {

    public static final EncryptionConfig PLAIN = EncryptionConfig.builder()
            .securityProtocol(DEFAULT_SECURITY_PROTOCOL)
            .build();

    String securityProtocol;

    String sslKeyPassword;
    String sslKeystoreCertificateChain;
    String sslKeystoreKey;
    String sslKeystoreLocation;
    String sslKeystorePassword;
    String sslTruststoreCertificates;
    String sslTruststoreLocation;
    String sslTruststorePassword;
    List sslEnabledProtocols;
    String sslKeystoreType;
    String sslProtocol;
    String sslProvider;
    String sslTruststoreType;
    List sslCipherSuites;
    String sslEndpointIdentificationAlgorithm;
    String sslEngineFactoryClass;
    String sslKeyManagerAlgorithm;
    String sslSecureRandomImplementation;
    String sslTrustManagerAlgorithm;

    public Properties asProperties() {
        val p = new Properties();
        populateMap(p);
        return p;
    }

    /**
     * Adds to the properties of a producer the keys to manage SSL encryption as provided by user
     * @param props producer properties to be completed
     * @return a new instance of {@link Properties} with the (optional) SSL properties
     */
    public Properties decorateProducer(Properties props) {
        return commonDecorate(props);
    }

    /**
     * Adds to the properties of a consumer the keys to manage SSL encryption as provided by user
     * @param props producer properties to be completed
     * @return a new instance of {@link Properties} with the (optional) SSL properties
     */
    public Properties decorateConsumer(Properties props) {
        return commonDecorate(props);
    }

    public Properties commonDecorate(Properties original) {
        val p = new Properties();
        p.putAll(original);
        populateMap(p);
        return p;
    }

    private void populateMap(Map<Object, Object> p) {
        p.put(SECURITY_PROTOCOL_CONFIG, securityProtocol != null ? securityProtocol : SecurityProtocol.SSL.name);
        ofNullable(sslKeyPassword).ifPresent(prop -> p.put(SSL_KEY_PASSWORD_CONFIG, prop));
        ofNullable(sslKeystoreCertificateChain).ifPresent(prop -> p.put(SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, prop));
        ofNullable(sslKeystoreKey).ifPresent(prop -> p.put(SSL_KEYSTORE_KEY_CONFIG, prop));
        ofNullable(sslKeystoreLocation).ifPresent(prop -> p.put(SSL_KEYSTORE_LOCATION_CONFIG, prop));
        ofNullable(sslKeystorePassword).ifPresent(prop -> p.put(SSL_KEYSTORE_PASSWORD_CONFIG, prop));
        ofNullable(sslTruststoreCertificates).ifPresent(prop -> p.put(SSL_TRUSTSTORE_CERTIFICATES_CONFIG, prop));
        ofNullable(sslTruststoreLocation).ifPresent(prop -> p.put(SSL_TRUSTSTORE_LOCATION_CONFIG, prop));
        ofNullable(sslTruststorePassword).ifPresent(prop -> p.put(SSL_TRUSTSTORE_PASSWORD_CONFIG, prop));
        ofNullable(sslEnabledProtocols).ifPresent(prop -> p.put(SSL_ENABLED_PROTOCOLS_CONFIG, prop));
        ofNullable(sslKeystoreType).ifPresent(prop -> p.put(SSL_KEYSTORE_TYPE_CONFIG, prop));
        ofNullable(sslProtocol).ifPresent(prop -> p.put(SSL_PROTOCOL_CONFIG, prop));
        ofNullable(sslProvider).ifPresent(prop -> p.put(SSL_PROVIDER_CONFIG, prop));
        ofNullable(sslTruststoreType).ifPresent(prop -> p.put(SSL_TRUSTSTORE_TYPE_CONFIG, prop));
        ofNullable(sslCipherSuites).ifPresent(prop -> p.put(SSL_CIPHER_SUITES_CONFIG, prop));
        ofNullable(sslEndpointIdentificationAlgorithm).ifPresent(prop -> p.put(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, prop));
        ofNullable(sslEngineFactoryClass).ifPresent(prop -> p.put(SSL_ENGINE_FACTORY_CLASS_CONFIG, prop));
        ofNullable(sslKeyManagerAlgorithm).ifPresent(prop -> p.put(SSL_KEYMANAGER_ALGORITHM_CONFIG, prop));
        ofNullable(sslSecureRandomImplementation).ifPresent(prop -> p.put(SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, prop));
        ofNullable(sslTrustManagerAlgorithm).ifPresent(prop -> p.put(SSL_TRUSTMANAGER_ALGORITHM_CONFIG, prop));
    }

    /**
     * Creates an {@link EncryptionConfig} object starting from the properties of a producer/consumer. It makes
     * no assumptions about the content. Useful if you have a configured client and want to mutuate
     * SSL properties directly from the configuration of that client
     *
     * @param props the properties of a producer/consumer
     * @return a usable {@link EncryptionConfig} object
     */
    public static EncryptionConfig createFromProperties(Properties props) {

        if (props == null) return PLAIN;
        val builder = EncryptionConfig.builder();
        ofNullable(props.get(SECURITY_PROTOCOL_CONFIG)).ifPresent(s -> builder.securityProtocol((String) s));
        ofNullable(props.get(SSL_KEY_PASSWORD_CONFIG)).ifPresent(s -> builder.sslKeyPassword((String) s));
        ofNullable(props.get(SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG)).ifPresent(s -> builder.sslKeystoreCertificateChain((String) s));
        ofNullable(props.get(SSL_KEYSTORE_KEY_CONFIG)).ifPresent(s -> builder.sslKeystoreKey((String) s));
        ofNullable(props.get(SSL_KEYSTORE_LOCATION_CONFIG)).ifPresent(s -> builder.sslKeystoreLocation((String) s));
        ofNullable(props.get(SSL_KEYSTORE_PASSWORD_CONFIG)).ifPresent(s -> builder.sslKeystorePassword((String) s));
        ofNullable(props.get(SSL_TRUSTSTORE_CERTIFICATES_CONFIG)).ifPresent(s -> builder.sslTruststoreCertificates((String) s));
        ofNullable(props.get(SSL_TRUSTSTORE_LOCATION_CONFIG)).ifPresent(s -> builder.sslTruststoreLocation((String) s));
        ofNullable(props.get(SSL_TRUSTSTORE_PASSWORD_CONFIG)).ifPresent(s -> builder.sslTruststorePassword((String) s));
        ofNullable(props.get(SSL_ENABLED_PROTOCOLS_CONFIG)).ifPresent(s -> builder.sslEnabledProtocols((List) s));
        ofNullable(props.get(SSL_KEYSTORE_TYPE_CONFIG)).ifPresent(s -> builder.sslKeystoreType((String) s));
        ofNullable(props.get(SSL_PROTOCOL_CONFIG)).ifPresent(s -> builder.sslProtocol((String) s));
        ofNullable(props.get(SSL_PROVIDER_CONFIG)).ifPresent(s -> builder.sslProvider((String) s));
        ofNullable(props.get(SSL_TRUSTSTORE_TYPE_CONFIG)).ifPresent(s -> builder.sslTruststoreType((String) s));
        ofNullable(props.get(SSL_CIPHER_SUITES_CONFIG)).ifPresent(s -> builder.sslCipherSuites((List) s));
        ofNullable(props.get(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG)).ifPresent(s -> builder.sslEndpointIdentificationAlgorithm((String) s));
        ofNullable(props.get(SSL_ENGINE_FACTORY_CLASS_CONFIG)).ifPresent(s -> builder.sslEngineFactoryClass((String) s));
        ofNullable(props.get(SSL_KEYMANAGER_ALGORITHM_CONFIG)).ifPresent(s -> builder.sslKeyManagerAlgorithm((String) s));
        ofNullable(props.get(SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG)).ifPresent(s -> builder.sslSecureRandomImplementation((String) s));
        ofNullable(props.get(SSL_TRUSTMANAGER_ALGORITHM_CONFIG)).ifPresent(s -> builder.sslTrustManagerAlgorithm((String) s));
        return builder.build();
    }

    public static EncryptionConfig createFromSystemProp() {
       return createFromProperties(System.getProperties());
    }
}
