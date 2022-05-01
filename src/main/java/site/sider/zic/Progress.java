package site.sider.zic;

import lombok.Data;

@Data
public class Progress {
    private Long read_rows;
    private Long read_bytes;
    private Long written_rows;
    private Long written_bytes;
    private Long total_rows_to_read;
}
