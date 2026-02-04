package com.streamflow.admin.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/**
 * Request DTO for creating a topic
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateTopicRequest {

    @NotBlank(message = "Topic name is required")
    @Pattern(regexp = "^[a-zA-Z0-9._-]+$", message = "Topic name must contain only alphanumeric, dots, underscores, and hyphens")
    private String name;

    @Min(value = 1, message = "Partition count must be at least 1")
    private int partitions;

    @Min(value = 1, message = "Replication factor must be at least 1")
    private int replicationFactor;
}
