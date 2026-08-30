"""
Silver quality: completeness.

Status: NOT IMPLEMENTED.
Critical fields named in the spec: email, customer_id, product_id.
Must flag NULLs; must not delete rows.
"""


def check_completeness() -> None:
    raise NotImplementedError("Silver completeness checks are not implemented yet.")


if __name__ == "__main__":
    check_completeness()
