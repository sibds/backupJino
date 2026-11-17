"""Escaped private diagnostics; these strings must never be used for filesystem access."""
import json

UNSAFE_REASONS={'INVALID_NAME_TYPE','EMPTY_NAME','DOT_SEGMENT','CONTROL_CHARACTER','EMBEDDED_SEPARATOR',
    'OUTSIDE_ALLOWED_ROOT','SOURCE_NAME_CHANGED','PATH_CHECK_FAILED','INVALID_TOKEN','PATH_NOT_IN_LISTING',
    'DIRECTORY_ALIAS','UNSAFE_DIRECTORY_TYPE','UNSAFE_EXPORT_OBJECT',
    'WINDOWS_CHARACTERS','TRAILING_DOT_OR_SPACE','WINDOWS_RESERVED_NAME','INVALID_UTF8_NAME'}


def display_path(value):
    if not isinstance(value,str):return '<invalid '+type(value).__name__+'>'
    value=value.encode('utf-8','backslashreplace').decode('utf-8')
    return json.dumps(value,ensure_ascii=False)[1:-1]


def child_display(parent,name):
    return display_path(parent+('/' if parent else '')+(name if isinstance(name,str) else '<invalid name>'))


def omission(path,error,kind='file'):
    return {'path':display_path(path),'kind':kind,'error_code':'REMOTE_PATH_UNSAFE',
            'reason':error.details.get('unsafe_reason','PATH_CHECK_FAILED'),
            **{key:value for key,value in error.details.items() if key in {'unsafe_reason','unsafe_name','unsafe_path','expected_name','actual_name','operation'}}}


def log_omission(notify,item,count):
    notify({'stage':'FILE_SKIPPED','error_code':'REMOTE_PATH_UNSAFE','skipped_path':item['path'],
            'skipped_kind':item['kind'],'skip_reason':item['reason'],'skipped_count':count,
            **{key:value for key,value in item.items() if key in {'unsafe_reason','unsafe_name','unsafe_path','expected_name','actual_name','operation'}}})
