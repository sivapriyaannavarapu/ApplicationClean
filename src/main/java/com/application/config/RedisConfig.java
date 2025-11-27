package com.application.config;
 
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
 
@Configuration
@EnableCaching
public class RedisConfig {
 
    // --- THIS IS THE CORRECTED BEAN --- ✅
    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        // Correctly handles Java 8 date/time types
        objectMapper.registerModule(new JavaTimeModule());
        // Writes dates as standard text (e.g., "2025-09-20T12:34:14")
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return objectMapper;
    }
 
    @Bean
    public Jackson2JsonRedisSerializer<Object> jackson2JsonRedisSerializer(ObjectMapper objectMapper) {
        return new Jackson2JsonRedisSerializer<>(objectMapper, Object.class);
    }
 
    @Bean
    @Primary
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory,
            Jackson2JsonRedisSerializer<Object> jacksonSerializer) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
 
        StringRedisSerializer stringSerializer = new StringRedisSerializer();
 
        template.setKeySerializer(stringSerializer);
        template.setValueSerializer(jacksonSerializer);
        template.setHashKeySerializer(stringSerializer);
        template.setHashValueSerializer(jacksonSerializer);
 
        template.afterPropertiesSet();
        return template;
    }
 
    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        RedisStandaloneConfiguration configuration = new RedisStandaloneConfiguration();
        configuration.setHostName("192.168.20.58");
        configuration.setPort(6379);
        configuration.setPassword(RedisPassword.of("Welcome@123"));
        return new LettuceConnectionFactory(configuration);
    }
    
    @Bean
    public RedisCacheManager cacheManager(RedisConnectionFactory connectionFactory,
                                          Jackson2JsonRedisSerializer<Object> jacksonSerializer) {
 
        RedisCacheConfiguration defaultConfig = RedisCacheConfiguration.defaultCacheConfig()
                .entryTtl(Duration.ofMinutes(10))
                .serializeValuesWith(SerializationPair.fromSerializer(jacksonSerializer));
 
        Map<String, RedisCacheConfiguration> cacheConfigurations = new HashMap<>();

        cacheConfigurations.put("states", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("batchesByOrientation", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("BloodGroupTypes", defaultConfig.entryTtl(Duration.ofMinutes(2)));
//        cacheConfigurations.put("orientationAndBatchDetails", defaultConfig.entryTtl(Duration.ofMinutes(2)));
//        cacheConfigurations.put("orientationFee", defaultConfig.entryTtl(Duration.ofMinutes(2)));
//      cacheConfigurations.put("getAppRange", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("allZones", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("foodTypes", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("occupations", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("sectors", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("orientationsByCampusAndClass", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("campusesByBusinessType", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("allCampuses", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("dgmCampuses", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("allstatustable", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("dgmEmployees", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("academicYears", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("citiesByState", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("zonesByCity", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("employeesByZone", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("cities", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("campusesByZone", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("mobileNumberByEmpId", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("citiesByDistrict", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("prosByCampus", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("districts", defaultConfig.entryTtl(Duration.ofMinutes(2)));
//        //dgmservice
        cacheConfigurations.put("getDgmforCampus", defaultConfig.entryTtl(Duration.ofMinutes(2)));
//        cacheConfigurations.put("pinCode", defaultConfig.entryTtl(Duration.ofMinutes(2)));
//        cacheConfigurations.put("campusdetails", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("admissionTypes", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("studentTypes", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("genders", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("districtsByState", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("mandalsByDistrict", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("citiesByDistrict", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("organizations", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("banksByOrganization", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("studentClasses", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("quotas", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("employees", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("concessionReasons", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("classesByCampus", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("schoolTypes", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("religions", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("castes", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("campusforzonalaccountant", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("campusesByCity", defaultConfig.entryTtl(Duration.ofMinutes(2)));
        cacheConfigurations.put("branchesByOrgAndBank", defaultConfig.entryTtl(Duration.ofMinutes(2)));

      
        return RedisCacheManager.builder(connectionFactory)
                .cacheDefaults(defaultConfig)
                .withInitialCacheConfigurations(cacheConfigurations)
                .build();
   }
}
 