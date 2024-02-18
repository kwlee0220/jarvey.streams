jarvey.streams


## 설치 과정

### 1. JDK 1.8 설치
설치 후, 환경 변수 `JAVA_HOME`를 JDK가 설치된 디렉토리 경로로 설정한다.

### 2. git 설치

### 3. Gradle 5.6.4 설치
설치 후, gradle 명령을 환경 변수 `PATH`에 추가한다.

### 4. 소스코드 Download

소스코드는 총 3개의 git repository로 구성되며 모두 공통 디렉토리 하에 download된다고
가정한다. 이 공통 디렉토리는 환경 변수 `$DEV_HOME`에 설정되었다고 가정한다.

#### 4.1 Dependency 프로젝트 [utils](https://github.com/kwlee0220/utils) 와 [utils.geo](https://github.com/kwlee0220/utils.geo)  Download
아래와 같이 `$DEV_HOME/commons` 디렉토리를 생성하고, `utils`와 `utils.geo` 프로젝트를
clone한다.

```
mkdir -p $DEV_HOME/common
cd $DEV_HOME/common/

git clone https://github.com/kwlee0220/utils.git
git clone https://github.com/kwlee0220/utils.geo.git
```

### 2. [jarvey.streams](https://github.com/kwlee0220/jarvey.streams) Download 및 빌드

#### 2.1 [jarvey.streams](https://github.com/kwlee0220/jarvey.streams) Download
```
mkdir -p $DEV_HOME/jarvey
cd $DEV_HOME/jarvey/
git clone https://github.com/kwlee0220/jarvey.streams.git
```

#### 2.2 `jarvey.streams` 빌드
```
# jarvey.streams 디렉토리로 이동
cd $DEV_HOME/jarvey/jarvey.streams

# jar 빌드
gradle assemble
```

#### 2.3 설정
```
# 생성된 jar 파일을 $DEV_HOME/jarvey/jarvey.streams/sbin 디렉토리로 복사
cp $DEV_HOME/jarvey/jarvey.streams/build/libs/jarvey.streams-<version>-all.jar $DEV_HOME/jarvey/jarvey.streams/sbin

# OS에 맞게 $DEV_HOME/jarvey/jarvey.streams/sbin 경로를 PATH에 추가

cd $DEV_HOME/jarvey/jarvey.streams/sbin
# create symbolic link (Linux 경우)
ln -s jarvey.streams-<version>-all.jar jarvey.streams.jar
# create symbolic link (Windows 경우)
mklink jarvey.streams.jar jarvey.streams-<version>-all.jar
```

#### 2.4 동작 확인
```
jarvey
jarvey format
```

#### 2.5 JDBC 설정

사용하는 DBMS에 맞게 jarvey configuration 파일(`mcmot_configs.yaml`)의 jdbc 정보를 설정한다.

```
# example
...
jdbc: postgresql:localhost:<jdbc_port>:<user_id>:<passwd>:<db_name>
...
```


