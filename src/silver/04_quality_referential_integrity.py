"""
Silver quality: referential integrity.

Status: NOT IMPLEMENTED.
orders.customer_id and orders.product_id must exist in parent tables when present.
"""


def check_referential_integrity() -> None:
    raise NotImplementedError(
        "Silver referential integrity checks are not implemented yet."
    )


if __name__ == "__main__":
    check_referential_integrity()
