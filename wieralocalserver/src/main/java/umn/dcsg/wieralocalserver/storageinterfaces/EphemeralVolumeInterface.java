package umn.dcsg.wieralocalserver.storageinterfaces;

/**
 * Created with IntelliJ IDEA.
 * User: ajay
 * Date: 26/02/13
 * Time: 1:36 PM
 * To change this template use File | Settings | File Templates.
 */
public class EphemeralVolumeInterface extends LocalDiskInterface {
    public EphemeralVolumeInterface(String ebsFolder) {
        super(ebsFolder, true);
    }
}
