all: ../lib/pq2jdbc.jar

../lib/pq2jdbc.jar: ru/postgrespro/pq2jdbc/*.java
	javac ru/postgrespro/pq2jdbc/*.java
	jar cvf ../lib/pq2jdbc.jar  ru/postgrespro/pq2jdbc/*.class

tgz:
	 rm -f ru/postgrespro/pq2jdbc/*.class ; cd ../.. ; tar cvzf pq2jdbc.tgz pq2jdbc
