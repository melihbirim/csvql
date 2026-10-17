# bash completion for csvql
#
# Install (pick one):
#   source /path/to/csvql.bash                      # from ~/.bashrc
#   cp csvql.bash ~/.local/share/bash-completion/completions/csvql
#
# Keep the flag list in sync with `csvql --help`; tests/completions_test.zig
# fails when a documented flag is missing here.

_csvql() {
    local cur prev
    cur=${COMP_WORDS[COMP_CWORD]}
    prev=
    if [ "$COMP_CWORD" -gt 0 ]; then
        prev=${COMP_WORDS[COMP_CWORD-1]}
    fi
    COMPREPLY=()

    # `csvql install` takes only --print.
    if [ "$COMP_CWORD" -ge 2 ] && [ "${COMP_WORDS[1]}" = install ]; then
        COMPREPLY=( $(compgen -W '--print' -- "$cur") )
        return 0
    fi

    case "$prev" in
        --root)
            # Comma-separated directory list: complete the segment being typed.
            local head= tail=$cur
            case "$cur" in
                *,*) head=${cur%,*},; tail=${cur##*,} ;;
            esac
            local d
            while IFS= read -r d; do
                COMPREPLY+=( "$head$d" )
            done < <(compgen -d -- "$tail")
            return 0
            ;;
        -o|--output|--audit|--schema|-s)
            COMPREPLY=( $(compgen -f -- "$cur") )
            return 0
            ;;
        -d|--delimiter|--threads)
            return 0
            ;;
    esac

    if [[ $cur == -* ]]; then
        COMPREPLY=( $(compgen -W '-h --help -v --version --no-header --no-input-header -o --output -d --delimiter --json --jsonl --table --no-table --markdown --wrap --threads --strict --schema --mcp --root --audit' -- "$cur") )
        return 0
    fi

    if [ "$COMP_CWORD" -eq 1 ]; then
        COMPREPLY=( $(compgen -W 'install' -- "$cur") )
    fi
    COMPREPLY+=( $(compgen -f -- "$cur") )
    return 0
}

complete -o filenames -F _csvql csvql
