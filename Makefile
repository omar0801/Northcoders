#################################################################################
#
# Makefile to build the project
#
#################################################################################

PROJECT_NAME = de-neural-de-normalisers
REGION = eu-west-2
PYTHON_INTERPRETER = python
WD=$(shell pwd)
PYTHONPATH=${WD}
SHELL := /bin/bash
PIP:=pip

## Create python interpreter environment.
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up VirtualEnv."
	( \
	    $(PIP) install -q virtualenv virtualenvwrapper; \
	    virtualenv venv --python=$(PYTHON_INTERPRETER); \
	)

# Define utility variable to help calling Python from the virtual environment
ACTIVATE_ENV := source venv/bin/activate

# Execute python related functionalities from within the project's environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

## Build the environment requirements
requirements: create-environment
##	$(call execute_in_env, $(PIP) install -r ./requirements.txt)

################################################################################################################
# Set Up
## Install bandit
setupreq:
	$(call execute_in_env, $(PIP) install -r ./requirements.txt)

bandit:
	$(call execute_in_env, $(PIP) install bandit)

## Install safety
safety:
	$(call execute_in_env, $(PIP) install safety)

## Install black
black:
	$(call execute_in_env, $(PIP) install black)


## Set up dev requirements (bandit, safety, black)
dev-setup: setupreq bandit safety black 
## $(call execute_in_env, $(PIP) install -r ./dev-requirements.txt)

# Build / Run

## Run the security test (bandit + safety)
security-test:
# $(call execute_in_env, safety scan ./requirements.txt)
	$(call execute_in_env, bandit -lll */*.py *c/*/*.py)

## Run the black code check
run-black:
	$(call execute_in_env, black  ./src/*.py ./test/*.py) 


## Run the unit tests
unit-test:
##	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -vv) ## Enable this line when we have tests to run

## Run all checks
run-checks: security-test run-black unit-test