################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../src/CommandBuffer.cpp \
../src/DatabaseEnvironment.cpp \
../src/KafkaWriter.cpp \
../src/MemoryException.cpp \
../src/OpCode.cpp \
../src/OpCode0501.cpp \
../src/OpCode0502.cpp \
../src/OpCode0504.cpp \
../src/OpCode0506.cpp \
../src/OpCode050B.cpp \
../src/OpCode0513.cpp \
../src/OpCode0514.cpp \
../src/OpCode0B02.cpp \
../src/OpCode0B03.cpp \
../src/OpCode0B04.cpp \
../src/OpCode0B05.cpp \
../src/OpCode0B06.cpp \
../src/OpCode0B08.cpp \
../src/OpCode0B0B.cpp \
../src/OpCode0B0C.cpp \
../src/OpCode1801.cpp \
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
../src/TransactionMap.cpp \
../src/Writer.cpp 

OBJS += \
./src/CommandBuffer.o \
./src/DatabaseEnvironment.o \
./src/KafkaWriter.o \
./src/MemoryException.o \
./src/OpCode.o \
./src/OpCode0501.o \
./src/OpCode0502.o \
./src/OpCode0504.o \
./src/OpCode0506.o \
./src/OpCode050B.o \
./src/OpCode0513.o \
./src/OpCode0514.o \
./src/OpCode0B02.o \
./src/OpCode0B03.o \
./src/OpCode0B04.o \
./src/OpCode0B05.o \
./src/OpCode0B06.o \
./src/OpCode0B08.o \
./src/OpCode0B0B.o \
./src/OpCode0B0C.o \
./src/OpCode1801.o \
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
./src/TransactionMap.o \
./src/Writer.o 

CPP_DEPS += \
./src/CommandBuffer.d \
./src/DatabaseEnvironment.d \
./src/KafkaWriter.d \
./src/MemoryException.d \
./src/OpCode.d \
./src/OpCode0501.d \
./src/OpCode0502.d \
./src/OpCode0504.d \
./src/OpCode0506.d \
./src/OpCode050B.d \
./src/OpCode0513.d \
./src/OpCode0514.d \
./src/OpCode0B02.d \
./src/OpCode0B03.d \
./src/OpCode0B04.d \
./src/OpCode0B05.d \
./src/OpCode0B06.d \
./src/OpCode0B08.d \
./src/OpCode0B0B.d \
./src/OpCode0B0C.d \
./src/OpCode1801.d \
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
./src/TransactionMap.d \
./src/Writer.d 


# Each subdirectory must supply rules for building sources it contributes
src/%.o: ../src/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -std=c++1y -I/opt/instantclient_11_2/sdk/include -I/opt/rapidjson/include -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


