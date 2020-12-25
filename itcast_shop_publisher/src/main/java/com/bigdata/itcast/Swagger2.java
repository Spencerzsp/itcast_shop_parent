package com.bigdata.itcast;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * Swagger2 配置
 * 地址 http://localhost:8070/swagger-ui.html
 */
@Configuration
@EnableSwagger2
public class Swagger2 {

//    @Value("${swagger.show}")
//    private boolean swaggerShow;


    @Bean
    public Docket createRestApi() {
        return new Docket(DocumentationType.SWAGGER_2)
                .enable(true)
                .apiInfo(apiInfo())
                .select()
                .apis(RequestHandlerSelectors.basePackage("com.bigdata.itcast.controller"))
                .paths(PathSelectors.any())
                .build();
    }
    private ApiInfo apiInfo() {
        return new ApiInfoBuilder()
                .title("购物车数据查询的Restful API")
//                .description("更多Spring Boot相关文章请关注：https://luischen.com/")
//                .termsOfServiceUrl("https://luischen.com/")
//                .contact("Spencer zhang")
                .version("1.0")
                .build();
    }
}
