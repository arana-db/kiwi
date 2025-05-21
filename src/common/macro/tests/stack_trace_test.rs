mod test {
    use common_macro::stack_trace_debug;
    use snafu::{ResultExt, Snafu};

    #[derive(Snafu)]
    #[snafu(visibility(pub))]
    #[stack_trace_debug]
    pub enum TestError {
        #[snafu(display("IO Error"))]
        Io {
            #[snafu(source)]
            error: std::io::Error,
            location: snafu::Location,
        },
    }

    // #[test]
    // fn test_stack_trace_debug() {
    //     let res = std::fs::read("not_exist").context(IoSnafu);
    //     assert_eq!(
    //         format!(
    //             "{}\n{}",
    //             "0: IO Error, at src/common/macro/tests/stack_trace_test.rs:19:46",
    //             "1: Os { code: 2, kind: NotFound, message: \"No such file or directory\" }"
    //         ),
    //         format!("{:?}", res.unwrap_err())
    //     );
    // }
}
