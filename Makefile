hw3: hw3_compile hw3_upload

hw3_compile:
	./gradlew shadowJar

hw3_upload:
	scp build/libs/bdc-assignments-all.jar torre.studenti.math.unipd.it:dev/bdc/
