package com.application.dto;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class GraphResponseDTO {

    private List<GraphBarDTO> graphBarData;   // year-wise issued & sold % data

}
