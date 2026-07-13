use super::*;

#[test]
fn renders_bounded_recovery_instruction() {
    let fragment = CodeCellCompletion::new("cell-1".to_string());

    assert_eq!(
        fragment.body(),
        "\n<cell_id>cell-1</cell_id>\n<instruction>The background code cell completed. Call functions.wait exactly once with this cell_id to retrieve its terminal result before continuing.</instruction>\n"
    );
}
