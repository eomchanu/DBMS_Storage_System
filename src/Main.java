import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.ResultSet;


public class Main {
    public static void main(String[] args) {
        System.setErr(System.out);

        try (Scanner sc = new Scanner(System.in)) {
            while(true) {
                System.out.println("1. 파일 생성");
                System.out.println("2. 레코드 삽입");
                System.out.println("3. 조인 질의");
                System.out.println("4. 프로그램 종료");
                System.out.print("원하는 작업을 선택하세요 (1~4): ");

                int choice;
                try {
                    choice = Integer.parseInt(sc.nextLine().trim());
                    if (choice < 1 || choice > 4) {
                        System.err.println("[오류] 1부터 4 사이의 숫자를 입력해주세요.\n");
                        continue;
                    }
                } catch (NumberFormatException e) {
                    System.out.flush();
                    System.err.println("[오류] 숫자를 입력해주세요.\n");
                    continue;
                }

                try {
                    switch (choice) {
                        case 1 -> {
                            System.out.print("데이터가 담긴 파일의 이름을 입력해주세요: ");
                            String inputFile = sc.nextLine();
                            DBStorageManager.createFileHeader(inputFile);
                            System.out.println("파일 및 테이블 생성 완료");
                        }
                        case 2 -> {
                            System.out.print("데이터가 담긴 파일의 이름을 입력해주세요: ");
                            String inputFile = sc.nextLine();
                            DBStorageManager.insertRecords(inputFile);
                        }
                        case 3 -> {
                            System.out.print("조인 질의 입력: ");
                            String sqlQuery = sc.nextLine();

                            System.out.println(">> 구현 Merge Join 결과:");
                            DBQueryProcessor.executeMergeJoin(sqlQuery);

                            System.out.println();
                            System.out.println(">> SQL 질의문 수행 결과:");
                            SQLUtil.executeSQLJoinQuery(sqlQuery);
                        }

                        case 4 -> {
                            System.out.println("프로그램을 종료합니다.");
                            System.exit(0);
                        }
                        // case 3 -> {
                        //     System.out.print("데이터가 담긴 파일의 이름을 입력해주세요: ");
                        //     String inputFile = sc.nextLine();
                        //     List<String> values = DBStorageManager.extractFieldValues(inputFile);
                        //     System.out.println("필드 값들:");
                        //     for (String val : values) {
                        //         System.out.println(val);
                        //     }
                        // }
                        // case 4 -> {
                        //     System.out.print("데이터가 담긴 파일의 이름을 입력해주세요: ");
                        //     String inputFile = sc.nextLine();
                        //     List<Record> records = DBStorageManager.getRecordsInRangeFromFile(inputFile);
                        //     System.out.println("범위 내 레코드:");
                        //     for (Record r : records) {
                        //         System.out.println(r);
                        //     }
                        // }
                        default -> System.out.println("유효하지 않은 선택입니다. 1부터 4 사이의 숫자를 입력해주세요.\n");
                    }
                } catch (java.nio.file.NoSuchFileException e) {
                    System.err.println("존재하지 않는 파일입니다.\n");
                } catch (IOException e) {
                    System.out.println("입출력 오류: " + e.getClass().getSimpleName() + " - " + e.getMessage());
                } catch (IllegalArgumentException e) {
                    System.err.println("입력 오류: " + e.getMessage() + "\n");
                } catch (Exception e) {
                    System.err.println("예상치 못한 오류가 발생했습니다: " + e.getMessage() + "\n");
                }

                System.out.println();
            }
        }
    }
}

class DBStorageManager {
    public static void createFileHeader(String fileDataFile) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(fileDataFile));

        if (lines.size() < 4) {
            throw new IllegalArgumentException("파일의 형식이 잘못되었습니다.");
        }

        String rawFilename = lines.get(0).trim();
        String outputFilename = rawFilename.toLowerCase().endsWith(Constants.FILE_EXTENSION) ? rawFilename : rawFilename + Constants.FILE_EXTENSION;

        int fieldCount = Integer.parseInt(lines.get(1).trim());

        List<String> fieldNames = Arrays.asList(lines.get(2).trim().split(Constants.DELIMITER));
        List<Integer> fieldSizes = new ArrayList<>();
        for (String sizeStr : lines.get(3).trim().split(Constants.DELIMITER)) {
            fieldSizes.add(Integer.parseInt(sizeStr));
        }

        if (fieldNames.size() != fieldCount || fieldSizes.size() != fieldCount) {
            throw new IllegalArgumentException("필드 개수와 이름/크기 수가 일치하지 않습니다.");
        }

        File header = new File();
        header.fieldNames = fieldNames;
        header.fieldSizes = fieldSizes;
        header.recordCount = 0;
        header.firstBlockOffset = Constants.BLOCK_SIZE;

        try (RandomAccessFile raf = new RandomAccessFile(outputFilename, "rw")) {
            header.writeFileHeader(raf);
        }

        SQLUtil.createMySQLTable(rawFilename, fieldNames, fieldSizes);
    }

    public static void insertRecords(String recordDataFile) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(recordDataFile));
        if (lines.size() < 2) {
            throw new IllegalArgumentException("파일 형식이 잘못되었습니다.");
        }

        String fileBaseName = lines.get(0).trim();
        String filename = fileBaseName + Constants.FILE_EXTENSION;
        int recordCount = Integer.parseInt(lines.get(1).trim());

        try (RandomAccessFile raf = new RandomAccessFile(filename, "rw")) {
            File header = new File();
            header.readFileHeader(raf);

            int fieldCount = header.fieldNames.size();
            List<String> recordLines = lines.subList(2, lines.size());

            if (recordLines.size() != recordCount) {
                throw new IllegalArgumentException("레코드 개수와 실제 데이터 줄 수가 일치하지 않습니다.");
            }

            for (int i = 0; i < recordCount; i++) {
                List<String> fields = List.of(recordLines.get(i).split(Constants.DELIMITER));
                if (fields.size() != fieldCount) {
                    throw new IllegalArgumentException("레코드 " + (i + 1) + "의 필드 개수가 맞지 않습니다: 기대 " + fieldCount + ", 실제 " + fields.size());
                }

                List<String> recordFields = new ArrayList<>();
                for (String field : fields) {
                    recordFields.add(field.equalsIgnoreCase("null") ? null : field);
                }
                SQLUtil.insertTuple(fileBaseName, fields);

                Record rec = new Record(recordFields);
                header.addRecord(raf, rec);
            }

            System.out.println(recordCount + "개 레코드 삽입 완료!");
        }
    }

    public static List<String> extractFieldValues(String metadataPath) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(metadataPath));
        if (lines.size() < 2) {
            throw new IllegalArgumentException("메타데이터 파일에 대상 파일명과 필드명이 포함되어야 합니다.");
        }

        String filename = lines.get(0).trim() + Constants.FILE_EXTENSION; // .bin 붙이기
        String targetField = lines.get(1).trim();

        List<String> extractedValues = new ArrayList<>();

        try (RandomAccessFile raf = new RandomAccessFile(filename, "r")) {
            File header = new File();
            header.readFileHeader(raf);

            int fieldIndex = header.fieldNames.indexOf(targetField);
            if (fieldIndex == -1) {
                throw new IllegalArgumentException("지정한 필드명이 존재하지 않습니다: " + targetField);
            }

            int currentBlockOffset = header.firstBlockOffset;
            while (currentBlockOffset != -1) {
                Block block = Block.readBlock(raf, currentBlockOffset, header.fieldSizes);
                for (Record r : block.records) {
                    String value = r.fields.get(fieldIndex);
                    extractedValues.add(Objects.requireNonNullElse(value, "null"));
                }
                currentBlockOffset = block.nextBlockOffset;
            }
        }

        return extractedValues;
    }

    public static List<Record> getRecordsInRangeFromFile(String queryFilePath) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(queryFilePath));
        if (lines.size() < 3) {
            throw new IllegalArgumentException("입력 파일에는 최소 3줄 (파일명, 최솟값, 최댓값)이 필요합니다.");
        }

        String fileBaseName = lines.get(0).trim();
        String filename = fileBaseName + Constants.FILE_EXTENSION;

        String minKey = lines.get(1).trim();
        String maxKey = lines.get(2).trim();

        List<Record> result = new ArrayList<>();

        try (RandomAccessFile raf = new RandomAccessFile(filename, "r")) {
            File header = new File();
            header.readFileHeader(raf);

            int currentBlockOffset = header.firstBlockOffset;
            while (currentBlockOffset != -1) {
                Block block = Block.readBlock(raf, currentBlockOffset, header.fieldSizes);
                for (Record record : block.records) {
                    String key = record.fields.get(0); // 첫 필드를 서치키로 간주
                    if (key != null && key.compareTo(minKey) >= 0 && key.compareTo(maxKey) <= 0) {
                        result.add(record);
                    }
                }
                currentBlockOffset = block.nextBlockOffset;
            }
        }

        return result;
    }

    private DBStorageManager() {}
}

class DBQueryProcessor {
    public static void executeMergeJoin(String sqlQuery) throws IOException {
        Pattern pattern = Pattern.compile(
                "from\\s+(\\w+)\\s*,\\s*(\\w+)\\s+where\\s+(\\w+)\\.(\\w+)\\s*=\\s*(\\w+)\\.(\\w+)(?:\\s+order\\s+by\\s+(\\w+))?",
                Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sqlQuery);

        if (!matcher.find()) {
            throw new IllegalArgumentException("SQL 문법이 잘못되었거나 지원되지 않는 문법입니다.");
        }

        String tableA = matcher.group(1);
        String tableB = matcher.group(2);

        String leftTable = matcher.group(3);
        String leftKey = matcher.group(4);
        // String rightTable = matcher.group(5);
        String rightKey = matcher.group(6);
        // String orderBy = matcher.group(7);

        // 조인 키 결정 (어느 키가 어느 테이블인지 확인)
        String keyA, keyB;
        if (leftTable.equalsIgnoreCase(tableA)) {
            keyA = leftKey;
            keyB = rightKey;
        } else {
            keyA = rightKey;
            keyB = leftKey;
        }

        String fileAPath = tableA + Constants.FILE_EXTENSION;
        String fileBPath = tableB + Constants.FILE_EXTENSION;

        try (RandomAccessFile rafA = new RandomAccessFile(fileAPath, "r");
            RandomAccessFile rafB = new RandomAccessFile(fileBPath, "r")) {

            File headerA = new File(); headerA.readFileHeader(rafA);
            File headerB = new File(); headerB.readFileHeader(rafB);

            int indexA = headerA.fieldNames.indexOf(keyA);
            int indexB = headerB.fieldNames.indexOf(keyB);

            int offsetA = headerA.firstBlockOffset;
            int offsetB = headerB.firstBlockOffset;

            Block blockA = (offsetA != -1) ? Block.readBlock(rafA, offsetA, headerA.fieldSizes) : null;
            Block blockB = (offsetB != -1) ? Block.readBlock(rafB, offsetB, headerB.fieldSizes) : null;

            List<Record> recordsA = (blockA != null) ? blockA.records : List.of();
            List<Record> recordsB = (blockB != null) ? blockB.records : List.of();

            int idxA = 0, idxB = 0;

            while (blockA != null && blockB != null) {
                if (idxA >= recordsA.size()) {
                    offsetA = blockA.nextBlockOffset;
                    blockA = (offsetA != -1) ? Block.readBlock(rafA, offsetA, headerA.fieldSizes) : null;
                    recordsA = (blockA != null) ? blockA.records : List.of();
                    idxA = 0;
                    continue;
                }

                if (idxB >= recordsB.size()) {
                    offsetB = blockB.nextBlockOffset;
                    blockB = (offsetB != -1) ? Block.readBlock(rafB, offsetB, headerB.fieldSizes) : null;
                    recordsB = (blockB != null) ? blockB.records : List.of();
                    idxB = 0;
                    continue;
                }

                Record ra = recordsA.get(idxA);
                Record rb = recordsB.get(idxB);

                String keyValA = ra.fields.get(indexA);
                String keyValB = rb.fields.get(indexB);
                int cmp = keyValA.compareTo(keyValB);

                if (cmp < 0) {
                    idxA++;
                } else if (cmp > 0) {
                    idxB++;
                } else {
                    // matchKey 기준 그룹 수집
                    String matchKey = keyValA;
                    List<Record> groupA = new ArrayList<>();
                    List<Record> groupB = new ArrayList<>();

                    // A 그룹 수집
                    while (blockA != null) {
                        while (idxA < recordsA.size()) {
                            Record r = recordsA.get(idxA);
                            if (!r.fields.get(indexA).equals(matchKey)) break;
                            groupA.add(r);
                            idxA++;
                        }
                        if (idxA < recordsA.size()) break;
                        offsetA = blockA.nextBlockOffset;
                        blockA = (offsetA != -1) ? Block.readBlock(rafA, offsetA, headerA.fieldSizes) : null;
                        recordsA = (blockA != null) ? blockA.records : List.of();
                        idxA = 0;
                        if (recordsA.isEmpty()) break;
                        if (!recordsA.get(0).fields.get(indexA).equals(matchKey)) break;
                    }

                    // B 그룹 수집
                    while (blockB != null) {
                        while (idxB < recordsB.size()) {
                            Record r = recordsB.get(idxB);
                            if (!r.fields.get(indexB).equals(matchKey)) break;
                            groupB.add(r);
                            idxB++;
                        }
                        if (idxB < recordsB.size()) break;
                        offsetB = blockB.nextBlockOffset;
                        blockB = (offsetB != -1) ? Block.readBlock(rafB, offsetB, headerB.fieldSizes) : null;
                        recordsB = (blockB != null) ? blockB.records : List.of();
                        idxB = 0;
                        if (recordsB.isEmpty()) break;
                        if (!recordsB.get(0).fields.get(indexB).equals(matchKey)) break;
                    }

                    // Cross product
                    for (Record a : groupA) {
                        for (Record b : groupB) {
                            List<String> joined = new ArrayList<>(a.fields);
                            joined.addAll(b.fields);
                            System.out.println(String.join(", ", joined));
                        }
                    }
                }
            }
        }
    }

    private DBQueryProcessor() {}
}

class SQLUtil {
    public static void createMySQLTable(String tableName, List<String> fieldNames, List<Integer> fieldSizes) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS `").append(tableName).append("` (\n");

        for (int i = 0; i < fieldNames.size(); i++) {
            String field = fieldNames.get(i);
            int size = fieldSizes.get(i);
            sb.append("  `").append(field).append("` CHAR(").append(size).append(")");
            // if (i == 0) sb.append(" PRIMARY KEY"); // 첫 필드는 주요 키로
            sb.append(i == fieldNames.size() - 1 ? "\n" : ",\n");
        }

        sb.append(");");

        try (
            Connection conn = DriverManager.getConnection(Constants.JDBC_URL, Constants.JDBC_USER, Constants.JDBC_PASSWORD);
            Statement stmt = conn.createStatement()
        ) {
            stmt.executeUpdate(sb.toString());
        } catch (SQLException e) {
            System.err.println("MySQL 테이블 생성 실패: " + e.getMessage());
        }
    }

    // 튜플 삽입 기능
    public static void insertTuple(String tableName, List<String> values) {
        String placeholders = String.join(", ", values.stream().map(_ -> "?").toArray(String[]::new));
        String query = "INSERT INTO `" + tableName + "` VALUES (" + placeholders + ")";

        try (
            Connection conn = DriverManager.getConnection(Constants.JDBC_URL, Constants.JDBC_USER, Constants.JDBC_PASSWORD);
            PreparedStatement pstmt = conn.prepareStatement(query)
        ) {
            for (int i = 0; i < values.size(); i++) {
                pstmt.setString(i + 1, values.get(i));
            }
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("튜플 삽입 실패: " + e.getMessage());
        }
    }

    // SQL 질의문 수행 및 결과 출력
    public static void executeSQLJoinQuery(String sqlQuery) {
        try (
            Connection conn = DriverManager.getConnection(Constants.JDBC_URL, Constants.JDBC_USER, Constants.JDBC_PASSWORD);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sqlQuery)
        ) {
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();

            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(rs.getString(i));
                    if (i < columnCount) System.out.print(", ");
                }
                System.out.println();
            }
        } catch (SQLException e) {
            System.err.println("SQL JOIN 수행 실패: " + e.getMessage());
        }
    }

    private SQLUtil() {}
}

class File {
    int recordCount;
    List<String> fieldNames;
    List<Integer> fieldSizes;
    int firstBlockOffset;

    public File() {
        this.recordCount = 0;
        this.fieldNames = new ArrayList<>();
        this.fieldSizes = new ArrayList<>();
        this.firstBlockOffset = -1;
    }

    // 파일 헤더 쓰기
    public void writeFileHeader(RandomAccessFile raf) throws IOException {
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
    public void readFileHeader(RandomAccessFile raf) throws IOException {
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
            this.writeFileHeader(raf);
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
        this.writeFileHeader(raf);
    }

    public String getFileName() {
        return fieldNames.getFirst().toLowerCase() + Constants.FILE_EXTENSION;
    }

    void printFileHeaderInfo() {
        System.out.println("레코드 개수: " + recordCount);
        System.out.println("필드 개수: " + fieldNames.size());
        for (int i = 0; i < fieldNames.size(); i++) {
            System.out.printf("필드 %d: %s (%d bytes)%n", i + 1, fieldNames.get(i), fieldSizes.get(i));
        }
        System.out.println("첫 블록 offset: " + firstBlockOffset);
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
            throw new EOFException("잘못된 블록 offset 요청: " + position + " (파일 길이: " + raf.length() + ")");
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
                System.err.println("레코드 offset이 파일 길이보다 큽니다: " + currentOffset);
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
                System.err.println("offset " + offset + "에 해당하는 레코드를 찾을 수 없습니다.");
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
    public final static int BLOCK_SIZE = 100;
    public final static int BLOCK_HEADER_SIZE = 12;
    public final static int FIXED_FIELD_NAME_SIZE = 20;

    public final static String FILE_EXTENSION = ".bin";
    public final static String DELIMITER = "\\s+";

    public static final String JDBC_URL = "jdbc:mysql://localhost:3306/DBMS_storage_system";
    public static final String JDBC_USER = "root";
    public static final String JDBC_PASSWORD = "20190564";

    private Constants() {}
}
