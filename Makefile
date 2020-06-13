hw3: hw3_compile hw3_upload

hw3_compile:
	rm -rf build
	./gradlew shadowJar

hw3_upload:
	scp build/libs/bdc-assignments-all.jar torre.studenti.math.unipd.it:dev/bdc/
