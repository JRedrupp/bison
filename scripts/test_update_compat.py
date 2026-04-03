import importlib.util
import pathlib
import tempfile
import unittest


MODULE_PATH = pathlib.Path(__file__).with_name("update_compat.py")
SPEC = importlib.util.spec_from_file_location("update_compat", MODULE_PATH)
update_compat = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(update_compat)


class UpdateCompatTests(unittest.TestCase):
    def test_collect_stubs_counts_only_full_method_stubs(self) -> None:
        mojo_source = """
struct DataFrame:
    def shift(self, axis: Int = 0) raises -> Self:
        if axis != 0:
            _not_implemented("DataFrame.shift")
        return self

    def apply(self, func: String) raises -> Self:
        _not_implemented("DataFrame.apply")
        return self

def concat(objs: Int) raises -> Int:
    _not_implemented("concat")
    return objs
""".strip()

        with tempfile.TemporaryDirectory() as tmpdir:
            bison_dir = pathlib.Path(tmpdir) / "bison"
            bison_dir.mkdir()
            (bison_dir / "_frame.mojo").write_text(mojo_source)

            original_dir = update_compat.BISON_DIR
            try:
                update_compat.BISON_DIR = bison_dir
                counts = update_compat.collect_stubs()
            finally:
                update_compat.BISON_DIR = original_dir

        self.assertEqual(counts, {"DataFrame": 1, "Reshape": 1})


if __name__ == "__main__":
    unittest.main()
