package com.ticketing.common.error;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class BaseRestControllerAdviceTest {

    record Body(@NotBlank String name) {
    }

    @RestController
    static class TestController {
        @PostMapping("/validate")
        String validate(@Valid @RequestBody Body body) {
            return "ok";
        }

        @GetMapping("/missing")
        String missing() {
            throw new ResourceNotFoundException("Event 42 does not exist.");
        }

        @GetMapping("/conflict")
        String conflict() {
            throw new ConflictException(ErrorCode.ORDER_NOT_READY, "Order is still being materialized.");
        }

        @GetMapping("/boom")
        String boom() {
            throw new IllegalStateException("secret internal detail");
        }
    }

    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        LocalValidatorFactoryBean validator = new LocalValidatorFactoryBean();
        validator.afterPropertiesSet();
        mvc = MockMvcBuilders.standaloneSetup(new TestController())
                .setControllerAdvice(new BaseRestControllerAdvice())
                .setValidator(validator)
                .build();
        MDC.put("correlationId", "c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e");
    }

    @AfterEach
    void cleanup() {
        MDC.clear();
    }

    @Test
    void validationErrorReturns400WithFieldErrors() throws Exception {
        mvc.perform(post("/validate").contentType(MediaType.APPLICATION_JSON).content("{\"name\":\"\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(jsonPath("$.errorCode").value("VALIDATION_ERROR"))
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.title").value("Validation Error"))
                .andExpect(jsonPath("$.type").value("https://api.ticketing.internal/errors/validation-error"))
                .andExpect(jsonPath("$.instance").value("/validate"))
                .andExpect(jsonPath("$.correlationId").value("c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e"))
                .andExpect(jsonPath("$.errors[0].field").value("name"));
    }

    @Test
    void malformedBodyReturns400() throws Exception {
        mvc.perform(post("/validate").contentType(MediaType.APPLICATION_JSON).content("{bad"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("VALIDATION_ERROR"));
    }

    @Test
    void notFoundReturns404() throws Exception {
        mvc.perform(get("/missing"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.errorCode").value("RESOURCE_NOT_FOUND"))
                .andExpect(jsonPath("$.detail").value("Event 42 does not exist."))
                .andExpect(jsonPath("$.errors").isEmpty());
    }

    @Test
    void conflictReturns409WithGivenCode() throws Exception {
        mvc.perform(get("/conflict"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode").value("ORDER_NOT_READY"))
                .andExpect(jsonPath("$.title").value("Order Not Ready"))
                .andExpect(jsonPath("$.type").value("https://api.ticketing.internal/errors/order-not-ready"));
    }

    @Test
    void unexpectedExceptionReturns500WithoutLeakingDetails() throws Exception {
        mvc.perform(get("/boom"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.errorCode").value("INTERNAL_SERVER_ERROR"))
                .andExpect(jsonPath("$.detail").value("An unexpected error occurred."))
                .andExpect(jsonPath("$.timestamp").exists());
    }

    @Test
    void unsupportedMethodKeepsFrameworkStatus() throws Exception {
        mvc.perform(get("/validate"))
                .andExpect(status().isMethodNotAllowed())
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"));
    }
}
