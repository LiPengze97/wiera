package umn.dcsg.wieralocalserver;

import umn.dcsg.wieralocalserver.storageinterfaces.StorageInterface;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import umn.dcsg.wieralocalserver.TierInfo.TIER_TYPE;
import static umn.dcsg.wieralocalserver.TierInfo.TIER_TYPE.*;


/**
 * Created by ajay on 3/23/14.
 */
public class Tiers {
    public KeyLocker m_tierLocker = null;
    HashMap<String, Tier> m_tierManagerByName;
    HashMap<TIER_TYPE, ArrayList> m_tierManagerByLevel;

    public Tiers(JSONArray jsonTiers, int nConn)
    {
        m_tierLocker = new KeyLocker();

        m_tierManagerByName = new HashMap<>();
        m_tierManagerByLevel = new HashMap<>();

        //Init list. this is for multiple locations of each tier.
        m_tierManagerByLevel.put(MEMORY, new ArrayList());
        m_tierManagerByLevel.put(SSD, new ArrayList());
        m_tierManagerByLevel.put(HDD, new ArrayList());
        m_tierManagerByLevel.put(CLOUD_STORAGE, new ArrayList());
        m_tierManagerByLevel.put(CLOUD_ARCHIVAL, new ArrayList());
        m_tierManagerByLevel.put(WIERA_INSTANCE, new ArrayList());

        JSONObject jsonTier;
        int nLen = jsonTiers.length();

        for (int i = 0; i < nLen; i++) {
            jsonTier = jsonTiers.getJSONObject(i);
            addTier(jsonTier, nConn);
        }
    }

    public TIER_TYPE getTierType(String strTierName) {
        ReentrantReadWriteLock lock = m_tierLocker.getLock(strTierName);

        try {
            lock.readLock().lock();
            Tier tier = m_tierManagerByName.get(strTierName);

            if (tier != null) {
                return tier.getTierType();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.readLock().unlock();
        }

        m_tierLocker.getLock(strTierName);


        return null;
    }

    public boolean addTier(JSONObject jsonTier, int nConn) {
        TIER_TYPE tier_type;
        Tier newTier;
        newTier = new Tier(jsonTier, nConn);

        String strTierName = newTier.getTierName();

        //Set by name
        m_tierManagerByName.put(strTierName, newTier);
        tier_type = newTier.getTierType();

        //Set by Tier. For now, only first tier interface will be used.
        m_tierManagerByLevel.get(tier_type).add(newTier);

        return true;
    }

    public StorageInterface getTierInterface(int level) {
        TIER_TYPE type = TIER_TYPE.values()[level];

        //For now we only use the first interface tier always.
        Tier tierManager = (Tier) getTierListWithType(type).get(0);

        if (tierManager != null) {
            return tierManager.getInterface();
        }

        return null;
    }

    public StorageInterface getTierInterface(String strTierName) {
        if (m_tierManagerByName.containsKey(strTierName) == true) {
            return m_tierManagerByName.get(strTierName).getInterface();
        }

        return null;
    }

    public ArrayList getTierListWithType(TIER_TYPE type) {
        return m_tierManagerByLevel.get(type);
    }

    public Tier getTier(String strTierName) {
        return m_tierManagerByName.get(strTierName);
    }

    public ArrayList getTierNameList() {
        return new ArrayList(m_tierManagerByName.keySet());
    }

    public ArrayList getTierList() {
        return new ArrayList(m_tierManagerByName.values());
    }
    //Nan
    public Tier getDefaultTier() {

        // Nan: before, the default value in policy is not used.
        // user designated has the first priority
        for (Map.Entry<String, Tier> entry : m_tierManagerByName.entrySet()) {
            if(entry.getValue().m_tierInfo.isDefault() == true){
                return entry.getValue();
            }
        }

        //Find persistent fastest one as default as multiple tiers can be set as a default
        //In here find persistent storage (e.g., SSD rather than memory)
        List lstCheckTierType = Arrays.asList(SSD, HDD, CLOUD_STORAGE, CLOUD_ARCHIVAL, MEMORY);
        List lstStorage;
        Tier tier;

        for (int i = 0; i < lstCheckTierType.size(); i++) {
            if (m_tierManagerByLevel.containsKey(lstCheckTierType.get(i)) == true) {
                lstStorage = m_tierManagerByLevel.get(lstCheckTierType.get(i));

                for (int j = 0; j < lstStorage.size(); j++) {
                    tier = (Tier) lstStorage.get(j);
                    if (tier.m_tierInfo.isDefault() == true) {
                        return tier;
                    }
                }
            }
        }

        //No storage tier is set to default
        //Then simply find the fastest persistent storage
        for (int i = 0; i < lstCheckTierType.size(); i++) {
            if (m_tierManagerByLevel.containsKey(lstCheckTierType.get(i)) == true) {
                lstStorage = m_tierManagerByLevel.get(lstCheckTierType.get(i));
                if (lstStorage.size() > 0) {
                    return (Tier) lstStorage.get(0);
                }
            }
        }


        return null;
    }
}