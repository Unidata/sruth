public class Whatever implements Runnable {
    private volatile String string;

    @Override
    public void run() {
        string = "whatever";
    }

    public String getString() {
        return string;
    }

    public void main(final String[] args) throws InterruptedException {
        final Whatever whatever = new Whatever();
        final Thread thread = new Thread(whatever);
        thread.start();
        thread.join();
        final String string = whatever.getString();
        System.out.println(string);
    }
}
