package com.ticketing.gateway.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Downstream service base URLs.
 */
@Component
public class GatewayRoutesProperties {

    @Value("${INVENTORY_SERVICE_URL:${inventory.service.url:http://localhost:8081}}")
    private String inventoryServiceUrl;

    @Value("${ORDER_SERVICE_URL:${order.service.url:http://localhost:8082}}")
    private String orderServiceUrl;

    @Value("${PAYMENT_SERVICE_URL:${payment.service.url:http://localhost:8083}}")
    private String paymentServiceUrl;

    public String getInventoryServiceUrl() {
        return inventoryServiceUrl;
    }

    public void setInventoryServiceUrl(String inventoryServiceUrl) {
        this.inventoryServiceUrl = inventoryServiceUrl;
    }

    public String getOrderServiceUrl() {
        return orderServiceUrl;
    }

    public void setOrderServiceUrl(String orderServiceUrl) {
        this.orderServiceUrl = orderServiceUrl;
    }

    public String getPaymentServiceUrl() {
        return paymentServiceUrl;
    }

    public void setPaymentServiceUrl(String paymentServiceUrl) {
        this.paymentServiceUrl = paymentServiceUrl;
    }
}

