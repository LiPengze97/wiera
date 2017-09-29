package umn.dcsg.wieralocalserver;

/**
 * Created with IntelliJ IDEA.
 * User: ajay
 * Date: 1/8/13
 * Time: 1:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class MalformedMessageException extends Exception {

	private static final long serialVersionUID = 896346508529097032L;

	@Override
	public String toString() {
		return "Data read doesn't appear to be a valid Message Object";
	}
}