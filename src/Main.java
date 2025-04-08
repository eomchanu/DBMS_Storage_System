import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Main {
    public static void main(String[] args) {
        String filename = "disk_data.bin";

        FileHeader header = new FileHeader();
        header.fieldNames = Arrays.asList("name", "age", "city");
        header.fieldSizes = Arrays.asList(10, 4, 20); // 총 34바이트 + 1 + 4 = 최소 39바이트 per record
        header.recordCount = 0;
        header.firstBlockOffset = Constants.BLOCK_SIZE;

        try (RandomAccessFile raf = new RandomAccessFile(filename, "rw")) {
            // 초기 헤더 기록
            header.writeHeader(raf);

            for (int i = 1; i <= 20; i++) {
                String name = "User" + i;
                String age = String.valueOf(20 + i);
                String city = "City" + i;
                Record rec = new Record(Arrays.asList(name, age, city));
                header.addRecord(raf, rec);
            }

            System.out.println("20개 레코드 삽입 완료!");

            // 전체 레코드 출력 (블록 탐색)
            FileHeader readHeader = new FileHeader();
            readHeader.readHeader(raf);

            int currentBlockOffset = readHeader.firstBlockOffset;
            int totalRead = 0;
            while (currentBlockOffset != -1) {
                Block block = Block.readBlock(raf, currentBlockOffset, readHeader.fieldSizes);
                System.out.println("=== 블록 시작 offset: " + currentBlockOffset + " ===");
                for (Record r : block.records) {
                    System.out.println(r);
                    totalRead++;
                }
                currentBlockOffset = block.nextBlockOffset;
            }

            System.out.println("총 읽은 레코드 수: " + totalRead);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

class FileHeader {
    int recordCount;
    List<String> fieldNames;
    List<Integer> fieldSizes;
    int firstBlockOffset;

    public FileHeader() {
        this.recordCount = 0;
        this.fieldNames = new ArrayList<>();
        this.fieldSizes = new ArrayList<>();
        this.firstBlockOffset = -1;
    }

    // 파일 헤더 쓰기
    public void writeHeader(RandomAccessFile raf) throws IOException {
        raf.seek(0);
        raf.writeInt(recordCount);
        raf.writeInt(fieldNames.size());

        // 필드 이름 기록 (고정 길이, 널 바이트 패딩)
        for (String name : fieldNames) {
            byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
            byte[] padded = new byte[Constants.FIXED_FIELD_NAME_SIZE];
            System.arraycopy(nameBytes, 0, padded, 0, Math.min(nameBytes.length, Constants.FIXED_FIELD_NAME_SIZE));
            raf.write(padded);
        }

        // 필드 크기 기록
        for (int size : fieldSizes) {
            raf.writeInt(size);
        }

        // 첫 블록 offset
        raf.writeInt(firstBlockOffset);

        // padding
        long writtenSize = 4 + 4 +
                ((long) Constants.FIXED_FIELD_NAME_SIZE * fieldNames.size()) +
                (4L * fieldSizes.size()) + 4;
        long padding = Constants.BLOCK_SIZE - writtenSize;

        for (int i = 0; i < padding; i++) {
            raf.writeByte(0x00);
        }
    }

    // 파일 헤더 읽기
    public void readHeader(RandomAccessFile raf) throws IOException {
        raf.seek(0);
        this.recordCount = raf.readInt();
        int fieldCount = raf.readInt();

        fieldNames = new ArrayList<>();
        fieldSizes = new ArrayList<>();

        // 필드 이름 읽기
        for (int i = 0; i < fieldCount; i++) {
            byte[] buffer = new byte[Constants.FIXED_FIELD_NAME_SIZE];
            raf.readFully(buffer);
            String name = new String(buffer, StandardCharsets.UTF_8).trim();
            fieldNames.add(name);
        }

        // 필드 크기 읽기
        for (int i = 0; i < fieldCount; i++) {
            fieldSizes.add(raf.readInt());
        }

        this.firstBlockOffset = raf.readInt();
        // 나머지 padding은 무시
    }

    public void addRecord(RandomAccessFile raf, Record record) throws IOException {
        int blockOffset = this.firstBlockOffset;
        Block block = null;

        // 파일 크기가 헤더밖에 없으면, 첫 블록이 아직 없는 상태
        if (raf.length() <= blockOffset) {
            block = new Block();
            block.addRecord(record, fieldSizes);

            // 블록 파일에 기록
            raf.seek(blockOffset);
            block.writeBlock(raf, blockOffset, fieldSizes);

            this.recordCount++;
            raf.seek(0);
            this.writeHeader(raf);
            return;
        }

        // 마지막 블록 탐색
        while (blockOffset != -1) {
            block = Block.readBlock(raf, blockOffset, fieldSizes);
            if (block.nextBlockOffset == -1) break;
            blockOffset = block.nextBlockOffset;
        }

        int estimatedSize = record.getSize(fieldSizes);
        int lastOffset = block.firstAvailableOffset(fieldSizes);
        int remaining = Constants.BLOCK_SIZE - lastOffset;

        if (remaining < estimatedSize) {
            // 새 블록 생성
            Block newBlock = new Block();
            newBlock.addRecord(record, fieldSizes);

            int newOffset = blockOffset + Constants.BLOCK_SIZE;
            block.nextBlockOffset = newOffset;

            // 기존 블록 갱신
            raf.seek(blockOffset);
            block.writeBlock(raf, blockOffset, fieldSizes);

            // 새 블록 기록
            raf.seek(newOffset);
            newBlock.writeBlock(raf, newOffset, fieldSizes);
        } else {
            // 현재 블록에 추가
            block.addRecord(record, fieldSizes);
            raf.seek(blockOffset);
            block.writeBlock(raf, blockOffset, fieldSizes);
        }

        // 레코드 수 증가 및 헤더 갱신
        this.recordCount++;
        raf.seek(0);
        this.writeHeader(raf);
    }
}

class Block {
    int recordCount;
    int nextBlockOffset;
    int firstRecordOffset;
    List<Record> records;

    public Block() {
        this.recordCount = 0;
        this.nextBlockOffset = -1;
        this.firstRecordOffset = Constants.BLOCK_HEADER_SIZE;
        this.records = new ArrayList<>();
    }

    public static Block readBlock(RandomAccessFile raf, int position, List<Integer> fieldSizes) throws IOException {
        if (position >= raf.length()) {
            throw new EOFException("📛 잘못된 블록 offset 요청: " + position + " (파일 길이: " + raf.length() + ")");
        }

        raf.seek(position);
        int recordCount = raf.readInt();
        int nextBlockOffset = raf.readInt();
        int firstRecordOffset = raf.readInt();

        Block block = new Block();
        block.recordCount = recordCount;
        block.nextBlockOffset = nextBlockOffset;
        block.firstRecordOffset = firstRecordOffset;

        int currentOffset = position + Constants.BLOCK_HEADER_SIZE;

        for (int i = 0; i < recordCount; i++) {
            if (currentOffset >= raf.length()) {
                System.err.println("❌ 레코드 offset이 파일 길이보다 큽니다: " + currentOffset);
                break;
            }
            Record record = Record.readRecord(raf, currentOffset, fieldSizes);
            block.records.add(record);
            currentOffset += record.getSize(fieldSizes);
        }

        return block;
    }

    public void writeBlock(RandomAccessFile raf, int position, List<Integer> fieldSizes) throws IOException {
        raf.seek(position);

        raf.writeInt(recordCount);
        raf.writeInt(nextBlockOffset);
        raf.writeInt(firstRecordOffset);

        int currentOffset = Constants.BLOCK_HEADER_SIZE + position;
        for (Record record : records) {
            currentOffset = record.writeRecord(raf, currentOffset, fieldSizes);
        }

        // padding
        long writtenSize = currentOffset - position;
        long padding = Constants.BLOCK_SIZE - writtenSize;

        for (int i = 0; i < padding; i++) {
            raf.writeByte(0x00);
        }
    }

    public void addRecord(Record newRecord, List<Integer> fieldSizes) {
        String newSearchKey = newRecord.fields.getFirst();

        // 블록이 비어있는 경우 처리
        if (records.isEmpty()) {
            newRecord.nextRecordOffset = -1; // 첫 번째 레코드는 다음이 없으므로 -1
            firstRecordOffset = Constants.BLOCK_HEADER_SIZE;
            records.add(newRecord);
            recordCount++;
            return;
        }

        int offset = firstRecordOffset;
        Record previous = null;
        Record current = null;

        // 논리적으로 연결된 순서를 따라 탐색
        while (offset != -1) {
            current = findRecordByOffset(offset, fieldSizes);
            if (current == null) {
                System.err.println("❌ 오류: offset " + offset + "에 해당하는 레코드를 찾을 수 없습니다.");
                break;
            }
            String currentKey = current.fields.getFirst();

            if (newSearchKey.compareTo(currentKey) < 0) {
                break;
            }

            previous = current;
            offset = current.nextRecordOffset;
        }

        // 새 레코드의 offset = 현재 블록 내 마지막 위치
        int newOffset = firstAvailableOffset(fieldSizes);
        newRecord.nextRecordOffset = (current != null) ? offset : -1;

        if (previous == null) {
            // 새 레코드가 논리적으로 첫 번째가 될 경우
            firstRecordOffset = newOffset;
        } else {
            previous.nextRecordOffset = newOffset;
        }

        records.add(newRecord);
        recordCount++;
    }

    // 레코드를 파일에 쓰기 전에 정확한 위치(offset)를 미리 계산하는 함수
    public int firstAvailableOffset(List<Integer> fieldSizes) {
        int offset = Constants.BLOCK_HEADER_SIZE;

        // 현재 블록에 저장된 모든 레코드의 크기를 합산하여 offset 계산
        for (Record rec : records) {
            offset += rec.getSize(fieldSizes);
        }
        return offset;
    }

    // offset으로 레코드를 찾는 보조 함수
    private Record findRecordByOffset(int offset, List<Integer> fieldSizes) {
        int current = Constants.BLOCK_HEADER_SIZE;
        for (Record rec : records) {
            if (current == offset) return rec;
            current += rec.getSize(fieldSizes);
        }
        return null;
    }
}

class Record {
    byte nullBitmap;       // 최대 8개 필드의 널 여부 저장
    List<String> fields;   // 가변 길이 필드값
    int nextRecordOffset;     // 다음 레코드 위치 포인터(offset)

    public Record(List<String> fields) {
        this.fields = fields;
        this.nextRecordOffset = -1;
        this.nullBitmap = calculateNullBitmap(fields);
    }

    // RandomAccessFile에서 레코드 읽기
    public static Record readRecord(RandomAccessFile raf, int position, List<Integer> fieldSizes) throws IOException {
        raf.seek(position);
        byte nullBitmap = raf.readByte();
        List<String> fields = new ArrayList<>();

        for (int i = 0; i < fieldSizes.size(); i++) {
            boolean isNull = ((nullBitmap >> (7 - i)) & 1) == 1;
            if (!isNull) {
                byte[] data = new byte[fieldSizes.get(i)];
                raf.readFully(data);
                fields.add(new String(data, StandardCharsets.UTF_8).trim());
            } else {
                fields.add(null);
            }
        }

        int nextRecordOffset = raf.readInt();
        Record record = new Record(fields);
        record.nextRecordOffset = nextRecordOffset;

        return record;
    }

    // 레코드를 RandomAccessFile로 기록
    public int writeRecord(RandomAccessFile raf, int position, List<Integer> fieldSizes) throws IOException {
        raf.seek(position);
        raf.writeByte(nullBitmap);

        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i) != null) {
                byte[] data = fields.get(i).getBytes(StandardCharsets.UTF_8);
                byte[] fixed = new byte[fieldSizes.get(i)];
                System.arraycopy(data, 0, fixed, 0, Math.min(data.length, fixed.length));
                raf.write(fixed);
            }
        }

        raf.writeInt(nextRecordOffset);
        return (int) raf.getFilePointer(); // 다음 레코드를 위한 현재 위치 반환
    }

    // 레코드 크기
    public int getSize(List<Integer> fieldSizes) {
        int size = 1; // nullBitmap
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i) != null) {
                size += fieldSizes.get(i); // 고정 필드 크기 사용
            }
        }
        size += 4; // nextRecordOffset
        return size;
    }

    // Null Bitmap 계산
    private byte calculateNullBitmap(List<String> fields) {
        byte bitmap = 0;
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i) == null)
                bitmap |= (byte) (1 << (7 - i));
        }
        return bitmap;
    }

    @Override
    public String toString() {
        return "Fields: " + fields + ", Next Record Offset: " + nextRecordOffset;
    }
}

class Constants {
    public final static int BLOCK_SIZE = 512;
    public final static int BLOCK_HEADER_SIZE = 12;
    public final static int FIXED_FIELD_NAME_SIZE = 20;

    private Constants() {}
}
