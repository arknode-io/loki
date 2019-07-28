.PHONY: default all build clean compile release shell upgrade

rebar='rebar3'

default: compile
all: clean compile test
compile:
	@$(rebar) compile
clean:
	@$(rebar) clean
cleanall:
	@$(rebar) clean -a
test:
	@$(rebar) do ct
shell:
	@$(rebar) shell
upgrade:
	@$(rebar) upgrade
