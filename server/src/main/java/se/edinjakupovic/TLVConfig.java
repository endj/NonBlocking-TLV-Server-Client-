package se.edinjakupovic;

public record TLVConfig(
        int headerSizeBytes,
        int maxBodySize
) {
}
