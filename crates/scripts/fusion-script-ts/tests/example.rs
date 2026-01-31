use deno_core::*;
use deno_core::error::JsError;

/// An op for summing an array of numbers. The op-layer automatically
/// deserializes inputs and serializes the returned Result & value.
#[op2]
fn op_sum(#[serde] nums: Vec<f64>) -> Result<f64, JsError> {
    // Sum inputs
    let sum = nums.iter().fold(0.0, |a, v| a + v);
    // return as a Result<f64, OpError>
    Ok(sum)
}

#[test]
fn test() {
    // Build a deno_core::Extension providing custom ops
    const DECL: OpDecl = op_sum();
    let ext = Extension {
        name: "my_ext",
        ops: std::borrow::Cow::Borrowed(&[DECL]),
        ..Default::default()
    };

    // Initialize a runtime instance
    let mut runtime = JsRuntime::new(RuntimeOptions {
        extensions: vec![ext],
        ..Default::default()
    });

    // Now we see how to invoke the op we just defined. The runtime automatically
    // contains a Deno.core object with several functions for interacting with it.
    // You can find its definition in core.js.
    runtime
        .execute_script(
            "<usage>",
            r#"
// Print helper function, calling Deno.core.print()
function print(value) {
  Deno.core.print(value.toString()+"\n");
}
let val = "32.519"

const parsedNumber = parseFloat(val)
val = "32.552"

print(`parsedNumber ${parsedNumber} ${val}`)

const arr = [1, 2, 3];
print("The sum of");
print(arr);
print("is");
print(Deno.core.ops.op_sum(arr));

// And incorrect usage
try {
  print(Deno.core.ops.op_sum(0));
} catch(e) {
  print('Exception:');
  print(e);
}
"#,
        )
        .unwrap();
}