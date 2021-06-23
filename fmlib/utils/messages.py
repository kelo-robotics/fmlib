import inflection


def format_msg(msg_dict):
    def _format_msg_keys(value):
        return {inflection.camelize(prop, False): format_msg(value)
                for prop, value in value.items()}

    if isinstance(msg_dict, dict):
        for prop, value in msg_dict.items():
            if isinstance(value, list):
                for i, element in enumerate(value):
                    if isinstance(element, dict):
                        value[i] = format_msg(element)
        return _format_msg_keys(msg_dict)
    else:
        return msg_dict


def format_document(doc_dict):
    def _format_doc_keys(doc):
        return {inflection.underscore(prop): format_document(value)
                for prop, value in doc.items()}

    if isinstance(doc_dict, dict):
        for prop, value in doc_dict.items():
            if isinstance(value, list):
                for i, element in enumerate(value):
                    if isinstance(element, dict):
                        value[i] = format_document(element)
        return _format_doc_keys(doc_dict)
    else:
        return doc_dict

