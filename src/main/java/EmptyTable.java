import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class EmptyTable {
  public static void main(String[] args) {
    System.out.println(Arrays.toString(args));
    try (final DBOptions options = new DBOptions()) {
      List<ColumnFamilyDescriptor>
          x =
          Arrays.asList("bucketTable", "keyTable", "metaTable", "dTokenTable",
                  "snapshotRenamedTable", "openFileTable",
                  "tenantAccessIdTable", "deletedDirectoryTable",
                  "tenantStateTable", "volumeTable", "multipartInfoTable",
                  "compactionLogTable", "prefixTable", "directoryTable",
                  "snapshotInfoTable", "openKeyTable",
                  "transactionInfoTable", "s3SecretTable", "userTable",
                  "deletedTable",
                  "principalToAccessIdsTable", "fileTable", "default").stream()
              .map(i -> new ColumnFamilyDescriptor(
                  i.getBytes(StandardCharsets.UTF_8))).collect(
                  Collectors.toList());
      List<ColumnFamilyHandle> ch = new ArrayList<>();
      // a factory method that returns a RocksDB instance
      try (final RocksDB db = RocksDB.open(options, args[0], x, ch);
           Options options1 = new Options()) {
        ColumnFamilyHandle handle = ch.get(14);
        System.out.println(new String(handle.getName()));
        try (RocksIterator iterator = db.newIterator(handle)) {
          iterator.seekToFirst();
          while (iterator.isValid()) {
            System.out.println(new String(iterator.key()));
            db.delete(handle, iterator.key());
            iterator.next();
          }
        }


      } catch (RocksDBException e) {
        // do some error handling
        throw new RuntimeException(e);
      }
    }
  }
}
