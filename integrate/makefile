.PHONY: env prepare clean
env: clean
	docker-compose up -d
prepare:
	bash prepare_env.sh
clean:
	docker-compose stop
	docker-compose rm --force
	rm -rf cephconf
