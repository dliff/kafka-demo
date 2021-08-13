package com.arctype;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TradeModel {
    public Integer id;
    public String ticker;
    public Integer price;
    public Integer quantity;
}
