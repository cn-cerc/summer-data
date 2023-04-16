package cn.cerc.db.core;

import java.util.Random;

public class AccountGenerator {

    public static void main(String[] args) {
        System.out.println(generateAccount());
    }

    public static String generateAccount() {
        String prefix = "220701";
        String suffix = generateSuffix();
        return prefix + suffix;
    }

    private static String generateSuffix() {
        Random random = new Random();
        char[] suffixArray = new char[4];
        int letterIndex = random.nextInt(4);

        for (int i = 0; i < 4; i++) {
            if (i == letterIndex) {
                suffixArray[i] = (char) ('A' + random.nextInt(26));
            } else {
                suffixArray[i] = (char) ('0' + random.nextInt(10));
            }
        }

        return new String(suffixArray);
    }
}