from jinja2 import pass_context, Template

@pass_context
def recursive_render(context, value, level=1) -> str:
    try:
        return Template(
            value if level == 1 else recursive_render(context, value, level - 1)
        ).render(context.get_all())
    except TypeError:
        return value
