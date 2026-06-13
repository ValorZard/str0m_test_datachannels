"""Rust CI pipeline."""
from __future__ import annotations

import harmont as hm


@hm.pipeline(
    "ci",
    env={"CI": "true", "CARGO_TERM_COLOR": "always"},
    triggers=[hm.push(branch="master"), hm.pull_request(branches="master")],
)
def ci() -> tuple[hm.Step, ...]:
    project = hm.rust.project(path=".")
    return (
        project.build(),
        project.build(
            target="wasm32-unknown-unknown",
            # By default, hm builds all workspace dependencies (through
            # `cargo build --workspace`) which fails:
            #
            # 194 | / compile_error!(concat!(
            # 195 | |     "The wasm32-unknown-unknown targets are not supported by default; \
            # 196 | |     you may need to enable the \"wasm_js\" configuration flag. Note \
            # 197 | |     that enabling the `wasm_js` feature flag alone is insufficient. \
            # 198 | |     For more information see: \
            # 199 | |     https://docs.rs/getrandom/", env!("CARGO_PKG_VERSION"), "/#webassembly-support"
            # 200 | | ));
            #     | |__^
            #
            # error: could not compile `getrandom` (lib) due to 1 previous error
            #
            # This is a stronger guarantee than the current GHA workflow --
            # which only builds datachannel-socket and its children.
            #
            # The native_peer crate is not actually built in GHA, but by
            # default, it was in harmont, and had the aforementioned failure.
            #
            # To solve the failure, you should read up on
            # https://docs.rs/getrandom/0.3.4/#webassembly-support
            #
            # In the immediate, I have disabled the check to match the GHA flow.
            workspace=False,
        ),
        project.test(),
        # The GHA flow did not have clippy enabled. Clippy fails left and right
        # so I temporarily disabled it.
        # Would highly suggest you enable it.
        #project.clippy(),
        # Same for formatting as with clippy.
        #project.fmt(),
    )
