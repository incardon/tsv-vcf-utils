package com.ukb.IGSB.TsvVcfUtils.init_db;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class TsvVcfFileMergerTest {

    public TsvVcfFileMergerTest() throws IOException {
    }

    public static long filesCompareByByte(File f1, File f2) throws IOException {

        GZIPfileInput s1 = new GZIPfileInput();
        GZIPfileInput s2 = new GZIPfileInput();

        BufferedReader fis1 = s1.open(f1.toString());
        BufferedReader fis2 = s2.open(f2.toString());

        int ch = 0;
        long pos = 1;
        while ((ch = fis1.read()) != -1) {
            if (ch != fis2.read()) {
                return pos;
            }
            pos++;
        }
        if (fis2.read() == -1) {
            return -1;
        } else {
            return pos;
        }
    }

    @Test
    public void merge_two_vcf() throws Exception {

        ArrayList<String> VcfFiles = new ArrayList<>();
        VcfFiles.add("test_data/vcf1.vcf.bgz");
        VcfFiles.add("test_data/vcf2.vcf.bgz");

        ArrayList<String> BedFiles = new ArrayList<>();
        BedFiles.add("test_data/bed1.bed");

        ArrayList<String> VcfColumns = new ArrayList<>();
        VcfColumns.add("AF:AF_IH");
        VcfColumns.add("VQSLOD:CONSEQ_CLASS");

        ArrayList<String> BedColumns = new ArrayList<>();
        BedColumns.add("3");

        ArrayList<String> BedFieldName = new ArrayList<>();
        BedFieldName.add("Anno1");

        new TsvVcfFileMerger(
                "mocker",
                "GRCh37",
                null,
                VcfFiles,
                BedFiles,
                null,
                VcfColumns,
                BedColumns,
                null,
                VcfColumns,
                BedFieldName,
                null,
                "vcf_database.vcf.bgz",
                true,
                false,
                false).run();

        File firstFile = new File("vcf_database.vcf.bgz");
        File secondFile = new File("test_data/vcf_database_test.vcf.bgz");

        long comp = filesCompareByByte(firstFile, secondFile);

        Assert.assertEquals(comp,-1);

    }


}