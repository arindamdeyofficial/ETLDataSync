
import chardet

class CommonHelper:
    def to_dict(obj):
        dict = {}
        for field in obj.__dict__.keys():
            if not field.startswith("_"):  # Exclude private attributes (optional)
                value = getattr(obj, field)
                dict[field] = value
        return dict
    def decode_prod_id(prod_id):
        """Decodes a product ID if necessary.

        Args:
            prod_id: The product ID to decode.

        Returns:
            The decoded product ID as a string.
        """

        if isinstance(prod_id, bytes):
            encoding = chardet.detect(prod_id)['encoding']
            return prod_id.decode(encoding)
        elif isinstance(prod_id, str):
            return prod_id
        else:
            raise TypeError("prod_id must be a string or bytes object")
