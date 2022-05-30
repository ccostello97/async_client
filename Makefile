do_mypy:
	mypy --strict async_client tests

do_black:
	black -l 80 async_client tests

do_pylint:
	pylint --max-line-length 80 async_client tests

do_flake8:
	flake8 --max-line-length 80 async_client tests

do_pytest:
	pytest tests

check:
	make do_mypy; make do_black; make do_pylint; make do_flake8; make do_pytest;
