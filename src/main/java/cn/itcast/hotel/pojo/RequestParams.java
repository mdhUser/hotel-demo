package cn.itcast.hotel.pojo;

import lombok.Data;

@Data
public class RequestParams {
    private String key;
    private Integer page = 1;
    private Integer size = 5;
    private String sortBy;
    private String brand;
    private String city;
    private String starName;
    private Long minPrice;
    private Long maxPrice;
    private String location;
}
