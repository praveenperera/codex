use super::ContextualUserFragment;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CodeCellCompletion {
    pub(crate) cell_id: String,
}

impl CodeCellCompletion {
    pub(crate) fn new(cell_id: String) -> Self {
        Self { cell_id }
    }
}

impl ContextualUserFragment for CodeCellCompletion {
    fn role(&self) -> &'static str {
        "user"
    }

    fn markers(&self) -> (&'static str, &'static str) {
        Self::type_markers()
    }

    fn type_markers() -> (&'static str, &'static str) {
        ("<code_cell_completion>", "</code_cell_completion>")
    }

    fn body(&self) -> String {
        format!(
            "\n<cell_id>{}</cell_id>\n<instruction>The background code cell completed. Call functions.wait exactly once with this cell_id to retrieve its terminal result before continuing.</instruction>\n",
            self.cell_id
        )
    }
}

#[cfg(test)]
#[path = "code_cell_completion_tests.rs"]
mod tests;
