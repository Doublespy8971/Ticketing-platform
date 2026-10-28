package com.ticketing.common.correlation;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class CorrelationIdFilterTest {

    private final CorrelationIdFilter filter = new CorrelationIdFilter();

    @AfterEach
    void cleanup() {
        MDC.clear();
    }

    private String[] run(MockHttpServletRequest request, MockHttpServletResponse response) throws Exception {
        AtomicReference<String> seenCorrelation = new AtomicReference<>();
        AtomicReference<String> seenUser = new AtomicReference<>();
        MockFilterChain chain = new MockFilterChain() {
            @Override
            public void doFilter(jakarta.servlet.ServletRequest req, jakarta.servlet.ServletResponse res) {
                seenCorrelation.set(MDC.get(CorrelationConstants.MDC_KEY));
                seenUser.set(MDC.get(CorrelationConstants.MDC_USER_ID_KEY));
            }
        };
        filter.doFilter(request, response, chain);
        return new String[] {seenCorrelation.get(), seenUser.get()};
    }

    @Test
    void reusesIncomingIdPutsItInMdcEchoesAndClears() throws Exception {
        String id = "c9b1d8e0-2f4a-4b6c-9d1e-3f5a7b9c0d2e";
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(CorrelationConstants.HTTP_HEADER, id);
        request.addHeader(CorrelationConstants.USER_ID_HEADER, "user-1");
        MockHttpServletResponse response = new MockHttpServletResponse();

        String[] seen = run(request, response);

        assertThat(seen[0]).isEqualTo(id);
        assertThat(seen[1]).isEqualTo("user-1");
        assertThat(response.getHeader(CorrelationConstants.HTTP_HEADER)).isEqualTo(id);
        assertThat(MDC.get(CorrelationConstants.MDC_KEY)).isNull();
        assertThat(MDC.get(CorrelationConstants.MDC_USER_ID_KEY)).isNull();
    }

    @Test
    void generatesUuidWhenHeaderMissing() throws Exception {
        MockHttpServletResponse response = new MockHttpServletResponse();
        String[] seen = run(new MockHttpServletRequest(), response);

        assertThat(UUID.fromString(seen[0])).isNotNull();
        assertThat(response.getHeader(CorrelationConstants.HTTP_HEADER)).isEqualTo(seen[0]);
    }

    @Test
    void generatesUuidWhenHeaderIsNotAUuid() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(CorrelationConstants.HTTP_HEADER, "not-a-uuid");
        MockHttpServletResponse response = new MockHttpServletResponse();

        String[] seen = run(request, response);

        assertThat(seen[0]).isNotEqualTo("not-a-uuid");
        assertThat(UUID.fromString(seen[0])).isNotNull();
    }
}
