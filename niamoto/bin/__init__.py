# coding: utf-8

from click.core import Group


class CustomCommandOrderGroup(Group):

    def __init__(self, *args, **kwargs):
        super(CustomCommandOrderGroup, self).__init__(*args, **kwargs)
        self.commands_display_dict = None

    def format_commands(self, ctx, formatter):
        """
        Custom commands formatter, groups the commands.
        """
        if self.commands_display_dict is None:
            return super(CustomCommandOrderGroup, self).format_commands(
                ctx,
                formatter
            )
        for group_key, commands in self.commands_display_dict.items():
            rows = []
            for command in commands:
                if command is None:
                    continue
                short_help = command.short_help or ''
                rows.append((command.name, short_help))
            if rows:
                with formatter.section(group_key):
                    formatter.write_dl(rows)
