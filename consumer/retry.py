import time


class TemporaryProcessingError(Exception):
    """
    Represents a temporary error that may succeed
    if the message is processed again.
    """
    pass


class PermanentProcessingError(Exception):
    """
    Represents an error where the message cannot
    be successfully processed.
    """
    pass


def process_with_retry(
    process_function,
    max_retries=3,
    initial_delay=1
):

    for attempt in range(1, max_retries + 1):

        try:

            return process_function()

        except TemporaryProcessingError as error:

            print(
                f"Temporary failure "
                f"(attempt {attempt}/{max_retries}): "
                f"{error}"
            )

            if attempt == max_retries:

                raise

            delay = initial_delay * (2 ** (attempt - 1))

            print(
                f"Retrying in {delay} second(s)..."
            )

            time.sleep(delay)

        except PermanentProcessingError:

            raise