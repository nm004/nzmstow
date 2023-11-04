import os
import os.path
import stat
from contextlib import ExitStack
from .glob import iglob

def rparse_gitignores(top, /, gitignore_name='.gitignore'):
    ignores = []
    for p,_,F in os.walk(os.path.normpath(top)):
        f = p + os.sep + gitignore_name
        try:
            if stat.S_ISREG(os.lstat(f).st_mode):
                ignores.append((p, f))
        except (OSError, ValueError, FileNotFoundError):
            pass

    with ExitStack() as stack:
        return parse_ignores( (p, stack.enter_context(open(f)).readlines()) for p, f in ignores )

def iglob_(p, root_dir):
    return iglob(p, root_dir=root_dir, recursive=True, include_hidden=True, follow_symlinks=False)

def parse_ignores(ignores):
    I = set()
    for base, ll in ignores:
        if are_ancestors_in(I, base):
            continue
        for l in ll:
            if not (l := l.rstrip('\n')):
                continue

            if l.startswith('#'):
                continue

            if (l := l.replace('\\\\', '\0')).endswith('\\'):
                continue

            l = l.replace(
                r'\*', '[*]'
            ).replace(
                r'\[', '[[]'
            ).replace(
                r'\?', '[?]'
            ).replace(
                r'\ ', '\n'
            ).rstrip(
                ' '
            ).replace(
                '\\', ''
            ).replace(
                '\0', '\\'
            ).replace(
                '\n', ' '
            ).replace(
                '/', os.sep
            )

            rm_from_I = l.startswith('!')
            l = l[rm_from_I:]

            l_parts = l.split(os.sep)
            if l == os.sep:
                continue

            if 2 * os.sep in l:
                continue

            if os.path.splitdrive(l)[0]:
                continue

            if not {'.', '..'}.isdisjoint(l_parts):
                continue

            if l_parts[-1] == '**':
                l = l + os.sep + '*'

            l = '**' + os.sep + l if l.find(os.sep) in { -1, len(l)-1 } else l
            l = l.removeprefix(os.sep)

            assert not os.path.isabs(l)
            assert not 2 * os.sep in l

            m = ( base + os.sep + i for i in iglob_(l, base) )
            if rm_from_I:
                for i in sorted(m):
                    if not are_ancestors_in(I, i):
                        try:
                            I.remove(i)
                        except KeyError:
                            pass
            else:
                I |= set(m)

    for i in I.copy():
        if stat.S_ISDIR(os.lstat(i).st_mode):
            I.update( i + os.sep + j for j in iglob_('**', i) )

    return I

def are_ancestors_in(I, i):
    return not { i[:j] for j,k in enumerate(i) if k == os.sep }.isdisjoint(I)
