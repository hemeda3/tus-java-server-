package me.desair.tus.server.upload.disk;

import me.desair.tus.server.exception.TusException;
import me.desair.tus.server.exception.UploadNotFoundException;
import me.desair.tus.server.upload.*;
import me.desair.tus.server.upload.concatenation.UploadConcatenationService;
import me.desair.tus.server.upload.concatenation.VirtualConcatenationService;
import me.desair.tus.server.util.Utils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;


public class UploadAzureStorageService  extends AbstractDiskBasedService implements UploadStorageService {

    private static final Logger log = LoggerFactory.getLogger(AbstractDiskBasedService.class);

    private static final String UPLOAD_SUB_DIRECTORY = "uploads";
    private static final String INFO_FILE = "info";
    private static final String DATA_FILE = "data";

    private Long maxUploadSize = null;
    private Long uploadExpirationPeriod = null;
    private UploadIdFactory idFactory;
    private UploadConcatenationService uploadConcatenationService;

    public UploadAzureStorageService(String storagePath) {
        super(storagePath + File.separator + UPLOAD_SUB_DIRECTORY);
        setUploadConcatenationService(new VirtualConcatenationService(this));
    }

    public UploadAzureStorageService(UploadIdFactory idFactory, String storagePath) {
        this(storagePath);
        Validate.notNull(idFactory, "The IdFactory cannot be null");
        this.idFactory = idFactory;
    }

    public void setIdFactory(UploadIdFactory idFactory) {
        Validate.notNull(idFactory, "The IdFactory cannot be null");
        this.idFactory = idFactory;
    }

    @Override
    public void setMaxUploadSize(Long maxUploadSize) {
        this.maxUploadSize = (maxUploadSize != null && maxUploadSize > 0 ? maxUploadSize : 0);
    }

    @Override
    public long getMaxUploadSize() {
        return maxUploadSize == null ? 0 : maxUploadSize;
    }

    @Override
    public UploadInfo getUploadInfo(String uploadUrl, String ownerKey) throws IOException {
        UploadInfo uploadInfo = getUploadInfo(idFactory.readUploadId(uploadUrl));
        if (uploadInfo == null || !Objects.equals(uploadInfo.getOwnerKey(), ownerKey)) {
            return null;
        } else {
            return uploadInfo;
        }
    }

    @Override
    public UploadInfo getUploadInfo(UploadId id) throws IOException {
        try {
            Path infoPath = getInfoPath(id);
            return Utils.readSerializable(infoPath, UploadInfo.class);
        } catch (UploadNotFoundException e) {
            return null;
        }
    }


    @Override
    public String getUploadURI() {
        return idFactory.getUploadURI();
    }



    @Override
    public UploadInfo create(UploadInfo info, String ownerKey) throws IOException {
        UploadId id = createNewId();

       // no need (SAS) createUploadDirectory(id);

        try {
            Path bytesPath = getBytesPath(id);

            //Create an empty file to storage the bytes of this upload
            // TODO create empty PUTBLOCK ->>> empty
            Files.createFile(bytesPath);

            //Set starting values
            info.setId(id);
            info.setOffset(0L);
            info.setOwnerKey(ownerKey);

            // todo update azure
            update(info);

            return info;
        } catch (UploadNotFoundException e) {
            //Normally this cannot happen
            log.error("Unable to create UploadInfo because of an upload not found exception", e);
            return null;
        }
    }



    @Override
    public void update(UploadInfo uploadInfo) throws IOException, UploadNotFoundException {
        // todo 4-
        Path infoPath = getInfoPath(uploadInfo.getId());
        Utils.writeSerializable(uploadInfo, infoPath);
    }


    @Override
    public UploadInfo append(UploadInfo upload, InputStream inputStream) throws IOException, TusException {
        return null;
    }



    @Override
    public InputStream getUploadedBytes(String uploadURI, String ownerKey) throws IOException, UploadNotFoundException {
        return null;
    }

    @Override
    public InputStream getUploadedBytes(UploadId id) throws IOException, UploadNotFoundException {
        return null;
    }

    @Override
    public void copyUploadTo(UploadInfo info, OutputStream outputStream) throws UploadNotFoundException, IOException {

    }

    @Override
    public void cleanupExpiredUploads(UploadLockingService uploadLockingService) throws IOException {

    }

    @Override
    public void removeLastNumberOfBytes(UploadInfo uploadInfo, long byteCount) throws UploadNotFoundException, IOException {

    }

    @Override
    public void terminateUpload(UploadInfo uploadInfo) throws UploadNotFoundException, IOException {

    }

    @Override
    public Long getUploadExpirationPeriod() {
        return null;
    }

    @Override
    public void setUploadExpirationPeriod(Long uploadExpirationPeriod) {

    }

    @Override
    public void setUploadConcatenationService(UploadConcatenationService concatenationService) {

    }

    @Override
    public UploadConcatenationService getUploadConcatenationService() {
        return null;
    }

    private Path getInfoPath(UploadId id) throws UploadNotFoundException {
        return  null;
    }

    private Path createUploadDirectory(UploadId id) throws IOException {
        return null;
    }



    private synchronized UploadId createNewId() throws IOException {
        UploadId id;
        do {
            id = idFactory.createId();
            //For extra safety, double check that this ID is not in use yet
        } while (getUploadInfo(id) != null);
        return id;
    }

    private Path getPathInUploadDir(UploadId id, String fileName) throws UploadNotFoundException {
        //Get the upload directory
        Path uploadDir = getPathInStorageDirectory(id);
        if (uploadDir != null && Files.exists(uploadDir)) {
            return uploadDir.resolve(fileName);
        } else {
            throw new UploadNotFoundException("The upload for id " + id + " was not found.");
        }
    }
    private Path getBytesPath(UploadId id) throws UploadNotFoundException {
        return getPathInUploadDir(id, DATA_FILE);
    }


}
