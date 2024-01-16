package cn.cerc.db.elasticsearch;

import java.util.Random;

public class RandomUtil {

    private static final Random RANDOM = new Random();

    public static String randomString(String chars, int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            int index = RANDOM.nextInt(chars.length());
            sb.append(chars.charAt(index));
        }
        return sb.toString();
    }

    public static int randomInt(int min, int max) {
        return RANDOM.nextInt(max - min + 1) + min;
    }

}
