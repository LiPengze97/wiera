package umn.dcsg.wieralocalserver.events;

import umn.dcsg.wieralocalserver.Constants;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/**
 * Created with IntelliJ IDEA.
 * User: ajay
 * Date: 07/05/13
 * Time: 7:15 PM
 * To change this template use File | Settings | File Templates.
 */
public class EventRegistry implements Runnable {

    //public static int CommandPort = 21234;
    public static int CommandPort = 0;
    ServerSocket m_commandSock;

    private ReentrantReadWriteLock m_registryBigLock = null;
    private HashMap<UUID, Event> m_eventRegistry = null;

    public EventRegistry() {
        m_registryBigLock = new ReentrantReadWriteLock();
        m_eventRegistry = new HashMap<>();
    }

    public UUID addEvent(Event event) {
        UUID eventID;
        WriteLock lock = m_registryBigLock.writeLock();
        try {
            lock.lock();
            eventID = UUID.randomUUID();
            event.setEventID(eventID);
            m_eventRegistry.put(eventID, event);
        } finally {
            lock.unlock();
        }
        return eventID;
    }

    public void deleteEvent(UUID uuid) {
        m_registryBigLock.readLock().lock();
        Event event = m_eventRegistry.get(uuid);
        m_registryBigLock.readLock().unlock();

        event.disable();

        m_registryBigLock.writeLock().lock();
        m_eventRegistry.remove(uuid);
        m_registryBigLock.writeLock().unlock();
    }

    public Object evaluateEvent(UUID uuid, Map<String, Object> eventParams, String strEventTrigger) {
        ReadLock lock = m_registryBigLock.readLock();
        Event event;

        try {
            lock.lock();
            event = m_eventRegistry.get(uuid);
        } finally {
            lock.unlock();
        }

        if (event == null)
            return null;

        return event.eventOccured(eventParams, strEventTrigger);
    }

    String listEvents() {
        String eventList;
        ReadLock lock = m_registryBigLock.readLock();
        try {
            lock.lock();
            eventList = m_eventRegistry.toString();
        } finally {
            lock.unlock();
        }
        return eventList;
    }

    public void closeSocket() {
        if (m_commandSock != null) {
            try {
                m_commandSock.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void run() {
        try {
            m_commandSock = new ServerSocket(CommandPort);
            m_commandSock.setSoTimeout(0);
        } catch (IOException e) {
            System.out.println("Failed to start Command Socket");
            e.printStackTrace();
            System.exit(1);
        }

        Socket newConnection = null;
        ObjectInputStream ois = null;
        ObjectOutputStream oos = null;
        while (true) {
            try {
                newConnection = m_commandSock.accept();
                ois = new ObjectInputStream(newConnection.getInputStream());
                oos = new ObjectOutputStream(newConnection.getOutputStream());

                // First read in the command which will be a string, we can then know 
                // what is the
                // next object
                Object objectRead = ois.readObject();
                String commandStr = (String) objectRead;
                if (commandStr.equals(Constants.LISTEVENTCOMMAND)) {
                    oos.writeObject(listEvents());
                    oos.flush();
                } else if (commandStr.equals(Constants.DELEVENTCOMMAND)) {
                    // What follows is the UUID of the event to be deleted
                    UUID eventID = (UUID) ois.readObject();
                    deleteEvent(eventID);
                } else if (commandStr.equals(Constants.PUTEVENTCOMMAND)) {
                    // What follows is an event
                    Event e = (Event) ois.readObject();
                    UUID eventID = addEvent(e);
                    oos.writeObject(eventID);
                    oos.flush();
                } else {
                    System.out.println("Unrecognized Command");
                }

                oos.writeObject("DONE");
                oos.flush();
                ois.close();
                oos.close();

            } catch (IOException e) {
                //System.out.println("Failed to accept connection on Command Socket");
                break;
            } catch (ClassNotFoundException e) {
                System.out.println("Failed to read command");
                e.printStackTrace();
            }
        }
    }
}