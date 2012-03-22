import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

class ModificationTimeExperiment {
    public static void main(final String[] args) throws IOException {
        final String homeDir = System.getProperty("user.home");
        assert homeDir != null;
        final Path path = Paths.get(homeDir);
        assert Files.exists(path);
        final BasicFileAttributeView view = Files.getFileAttributeView(path,
                BasicFileAttributeView.class);
        assert view != null;
        final BasicFileAttributes attributes = view.readAttributes();
        assert attributes != null;
        FileTime fileTime = attributes.lastModifiedTime();
        assert fileTime != null; // this fails
        fileTime = (FileTime) Files.getAttribute(path, "lastModifiedTime");
        assert fileTime != null; // this also fails
    }
}
