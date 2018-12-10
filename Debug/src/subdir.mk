################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../src/DatabaseEnvironment.cpp \
../src/JsonBuffer.cpp \
../src/KafkaWriter.cpp \
../src/MemoryException.cpp \
../src/OpCode.cpp \
../src/OpCode0501.cpp \
../src/OpCode0502.cpp \
../src/OpCode0504.cpp \
../src/OpCode0506.cpp \
../src/OpCode050B.cpp \
../src/OpCode0B02.cpp \
../src/OpCode0B03.cpp \
../src/OpenLogReplicator.cpp \
../src/OracleColumn.cpp \
../src/OracleEnvironment.cpp \
../src/OracleObject.cpp \
../src/OracleReader.cpp \
../src/OracleReaderRedo.cpp \
../src/OracleStatement.cpp \
../src/RedoLogException.cpp \
../src/RedoLogRecord.cpp \
../src/Thread.cpp \
../src/Transaction.cpp \
../src/TransactionBuffer.cpp \
../src/TransactionChunk.cpp \
../src/TransactionHeap.cpp \
../src/TransactionMap.cpp 

OBJS += \
./src/DatabaseEnvironment.o \
./src/JsonBuffer.o \
./src/KafkaWriter.o \
./src/MemoryException.o \
./src/OpCode.o \
./src/OpCode0501.o \
./src/OpCode0502.o \
./src/OpCode0504.o \
./src/OpCode0506.o \
./src/OpCode050B.o \
./src/OpCode0B02.o \
./src/OpCode0B03.o \
./src/OpenLogReplicator.o \
./src/OracleColumn.o \
./src/OracleEnvironment.o \
./src/OracleObject.o \
./src/OracleReader.o \
./src/OracleReaderRedo.o \
./src/OracleStatement.o \
./src/RedoLogException.o \
./src/RedoLogRecord.o \
./src/Thread.o \
./src/Transaction.o \
./src/TransactionBuffer.o \
./src/TransactionChunk.o \
./src/TransactionHeap.o \
./src/TransactionMap.o 

CPP_DEPS += \
./src/DatabaseEnvironment.d \
./src/JsonBuffer.d \
./src/KafkaWriter.d \
./src/MemoryException.d \
./src/OpCode.d \
./src/OpCode0501.d \
./src/OpCode0502.d \
./src/OpCode0504.d \
./src/OpCode0506.d \
./src/OpCode050B.d \
./src/OpCode0B02.d \
./src/OpCode0B03.d \
./src/OpenLogReplicator.d \
./src/OracleColumn.d \
./src/OracleEnvironment.d \
./src/OracleObject.d \
./src/OracleReader.d \
./src/OracleReaderRedo.d \
./src/OracleStatement.d \
./src/RedoLogException.d \
./src/RedoLogRecord.d \
./src/Thread.d \
./src/Transaction.d \
./src/TransactionBuffer.d \
./src/TransactionChunk.d \
./src/TransactionHeap.d \
./src/TransactionMap.d 


# Each subdirectory must supply rules for building sources it contributes
src/%.o: ../src/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -std=c++1y -I/opt/oracle/instantclient_11_2/sdk/include -I/opt/rapidjson/include -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


