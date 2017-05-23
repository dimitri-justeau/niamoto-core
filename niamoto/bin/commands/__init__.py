# coding: utf-8

from click.decorators import update_wrapper


def copy_parent_context(func):
    def _func(context, *args, **kwargs):
        context.params = context.parent.params
        return func(context, *args, **kwargs)
    return update_wrapper(_func, func)
