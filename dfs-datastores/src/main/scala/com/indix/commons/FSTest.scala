package com.indix.commons

import org.joda.time.DateTime

trait FSTest extends FS {
  def defaultWhiteList = Iterator("www.amazon.com")
  def defaultBlackList = Iterator(
    "http://www.drugstore.com/.*(\\?|\\&)catid=[0-9]+.*",
    "http://www.walgreens.com/.*(#([0-9]+)){2,}$",
    "http://www.walgreens.com/[^#]+$",
    "http://www.microsoftstore.com/store/((?!msusa/en_US).)*",
    "http://www.bluenile.com/[a-z]{2}/.*",
    "http://www.sierratradingpost.com/(?!.*utm_source=Indix&utm_medium=Indix.*#).*",
    "http://shopping.hp.com/.*SearchParameter=.*",
    "http://shopping.hp.com/en_US/home-office/-/products/Laptops/HP%20ENVY;pgid=vzFQh34DjJNSRpfVpshk3QwV0000KOWeCkIz;sid=gt7U1mQtaNv61jdT2b2DVr0isnHoQlSCvkoj2TLNBVKeFwS9xRdKSjgm",
    "http://shopping.hp.com/en_US/home-office/-/products/Laptops/HP%20ENVY;pgid=vzFQh34DjJNSRpfVpshk3QwV0000KOWeCkIz;sid=gt7U1mQtaNv61jdT2b2DVr0isnHoQlSCvkoj2TLNBVKeFwS9xRdKSjgm",
    "http://shopping.hp.com/laptops%20&%20hybrids/top+deals",
    "http://shopping.hp.com/laptops%20&%20hybrids/$600+-+$799",
    "http://shopping.hp.com/en_US/home-office/-/products/Laptops/HP%20ENVY;pgid=vzFQh34DjJNSRpfVpshk3QwV0000wtZ8ylnB;sid=ZkboBqc9CTL6BvRDNd72i38y4cqix5eSWtIu4etA4cqix8etIY-zPcW4",
    "http://shopping.hp.com/laptops%20&%20hybrids/envy?HP-ENVY-TouchSmart-15-j050us-Quad-Edition-Notebook-PC",
    "http://shopping.hp.com/en_US/home-office/-/products/Laptops/HP%20ENVY;pgid=EdNQvc4lM1hSRpWR7DDy74uK0000kvPvtdvQ;sid=i28V_4Tv7lY4_9Z_XwgIclzgu8Apa7RAt_vJRt-ZDONfPgpu_740BMWa",
    "http://shopping.hp.com/laptops%20&%20hybrids/high+performance",
    "http://shopping.hp.com/desktops%20&%20all-in-ones/all-in-one+touch",
    "http://shopping.hp.com/laptops%20&%20hybrids/ready+to+ship",
    "http://shopping.hp.com/laptops%20&%20hybrids/touchscreen",
    "http://shopping.hp.com/laptops%20&%20hybrids/under+15+inches",
    "http://shopping.hp.com/en_US/home-office/-/products/Laptops/HP%20ENVY;pgid=_HlQlIZ83ghSRpyZaigejugx0000D4Q0IHli;sid=mTgATwhCDmsmT1t4NtMdwtBNHrRKjo_O0_lRN1UWHrRKjiC05l2tPBou",
    "http://shopping.hp.com/en_US/home-office/-/products/Printers/HP%20Officejet%20Pro;pgid=Bb5waLgKrp9SRpN9sopouKXe0000c9tFRDyV;sid=BTFUZDXQThVdZGdAiekG5Ozfgr0epbJcT_AfQn-Pgr0epaMIMhGYRnC0",
    "http://shopping.hp.com/en_US/home-office/-/products/Printers/HP%20Officejet%20Pro",
    "http://shopping.hp.com/printers/multifunction+printers",
    "http://shopping.hp.com/laptops%20&%20hybrids/under+17+inches",
    "http://shopping.hp.com/printers/duplex+printing",
    "http://shopping.hp.com/laptops%20&%20hybrids/over+17+inches",
    "http://shopping.hp.com/printers/inkjet+printers",
    "http://shopping.hp.com/en_US/home-office/-/products/Laptops/HP%20Pavilion;pgid=vzFQh34DjJNSRpfVpshk3QwV0000oSUBj2MJ;sid=ema3ITRzKG68IWcO2pGprOx8SsmLtQTcRvJacHWY_er94FTjPa-3qXGJ",
    "http://shopping.hp.com/en_US/home-office/-/products/Printers/HP%20Officejet%20Pro;pgid=Hftw5H9FSPtSRpjf9E9rsAgd0000QCqimYtZ;sid=vRiXVYLjtHueVdHCO0GL2FrsjberwbJMgYzcc8i8OpTdlKoVwn0JT-gg",
    "http://shopping.hp.com/laptops%20&%20hybrids/everyday+computing",
    "http://shopping.hp.com/en_US/home-office/-/products/Desktops/HP%20ENVY;pgid=gj1Aupo5AtdSRpy_p5gtn3WU00004eEAfN6t;sid=xzNQbqoL1jZQbvh3IWMC7nME95xs-pqk-6cBFvdfQL8ar8qbgPpvZecE",
    "http://shopping.hp.com/laptops%20&%20hybrids/4+-+6+lbs",
    "http://shopping.hp.com/desktops%20&%20all-in-ones/envy",
    "http://shopping.hp.com/en_US/home-office/-/products/Desktops/HP%20ENVY",
    "http://shopping.hp.com/en_US/home-office/-/products/Desktops/HP%20Pavilion;pgid=X59woxn..FlSRpJh43fHDktP0000NbZ2Omex;sid=jGGbEGiuvQ6_EDvTEl3OkLGhvM6nhFgBsPVd9yTTC-3R0f52u0E04hjw",
    "http://shopping.hp.com/laptops%20&%20hybrids/$200+-+$399",
    "http://shopping.hp.com/desktops%20&%20all-in-ones/top+deals",
    "http://shopping.hp.com/desktops%20&%20all-in-ones/$400+-+$599",
    "http://shopping.hp.com/printers/business",
    "http://shopping.hp.com/printers/new+products",
    "http://shopping.hp.com/desktops%20&%20all-in-ones/intel",
    "http://shopping.hp.com/desktops%20&%20all-in-ones/$800+-+$999",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Monitors;pgid=gj1Aupo5AtdSRpy_p5gtn3WU0000UV8o1v1l;sid=MnsVlsPwfSAblpGNfNsJGxv_AtQpAvNfDu_TcY-NtfdfV6NgdbItfZRP",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Monitors;pgid=Hftw5H9FSPtSRpjf9E9rsAgd000000Jbwo3t;sid=FvS31Y5XHc-11d12k_3iVVdYJluLQb74KmBahM-8kXj9FADWYiXdBvZP",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Monitors;pgid=hG1QbPXdNm5SRpB38hTLuiui0000RU9AsxiK;sid=IzC82ovw8Ve62tjTWYHqWlL_E5-ATrtfH6RL1d0QpLz2GwVxV-ERqZmc",
    "http://shopping.hp.com/en_US/home-office/-/products/Laptops/HP;pgid=Cb9gJGxbPONSRpX.PKEA2f1i0000JiS3Bk5U;sid=wcuh8vgAwZzk8qs640S4fyAPRkfrM3-MiwrwiqVURkfrM27Y9usMgeps",
    "http://shopping.hp.com/desktops%20&%20all-in-ones/students",
    "http://shopping.hp.com/en_US/home-office/-/products/Care_Packs/Care%20Packs%20for%20Laptops;pgid=hG1QbPXdNm5SRpB38hTLuiui0000n5J-t4pM;sid=pT1rLrFkMBt7LuJHgmQ9rmhrlZJXuoHLmak6VuwwIrEh79H04vT1NNun",
    "http://shopping.hp.com/tablets/android",
    "http://shopping.hp.com/desktops%20&%20all-in-ones/all+customizable+pcs",
    "http://shopping.hp.com/en_US/home-office/-/products/Printers/HP%20LaserJet;pgid=3KJg_5B8rYFSRpu0LoZaqKFZ0000dKdD3siQ;sid=S67jzeP1RtvpzbDD0Nv_QDv6zCKpDGR5AW8_dLiDzCKpDG10P3_bJrRK",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Software;pgid=Hftw5H9FSPtSRpjf9E9rsAgd0000_FycsSiL;sid=JPPnir3Ji5ziiu7r4jL7B2XGFFzbHo1mGGcQhespo3-tSzNIUCLW6uKu",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Software;pgid=3KJg_5B8rYFSRpu0LoZaqKFZ0000dKdD3siQ;sid=S67jzeP1RtvpzbDD0Nv_QDv6zCKpDGR5AW8_dLiDzCKpDG10P3_bJrRK",
    "http://shopping.hp.com/printers/print+scan+and+copy",
    "http://shopping.hp.com/en_US/home-office/-/products/Laptops/Chromebook",
    "http://shopping.hp.com/printers/black+and+white+printers",
    "http://shopping.hp.com/en_US/home-office/-/products/Care_Packs/Care%20Packs%20for%20Desktops;pgid=Hftw5H9FSPtSRpjf9E9rsAgd0000YqSguRih;sid=PMVXt8w7hfcFt58ZBahKOhQ0u0kddku3dgSLDpdNu0kddkK6SBQHAPKc",
    "http://shopping.hp.com/en_US/home-office/-/products/Printers/HP%20Officejet;pgid=vzFQh34DjJNSRpfVpshk3QwV0000L63k5jbX;sid=d4ci5MZPAsMF5JUx-T51ZB9A8AtoJUHDPUbkA4oy8AtoJabfME4y952n",
    "http://shopping.hp.com/laptops%20&%20hybrids/essential+home",
    "http://shopping.hp.com/en_US/home-office/-/products/Care_Packs/Care%20Packs%20for%20Printers;pgid=vzFQh34DjJNSRpfVpshk3QwV0000eFKk6wDz;sid=cSC_MP_CvFS4MKy88aHtsCbNQY-DpM9tTbRIP6ki9qz18Z9SNumey763",
    "http://shopping.hp.com/printers/print+only",
    "http://shopping.hp.com/desktops%20&%20all-in-ones/amd",
    "http://shopping.hp.com/en_US/home-office/-/products/Tablets/Slate;pgid=vzFQh34DjJNSRpfVpshk3QwV0000ll-qiddU;sid=EUYP4ieEgwIe4nT6n7URb_-LIekzdhcrLdJExG3blspFIw9ybiNlMV-c",
    "http://shopping.hp.com/desktops%20&%20all-in-ones/$600+-+$799",
    "http://shopping.hp.com/tablets/16gb",
    "http://shopping.hp.com/printers/hp+instant+ink+eligible",
    "http://shopping.hp.com/laptops%20&%20hybrids/chrome+os",
    "http://shopping.hp.com/en_US/home-office/-/products/Printers/HP%20Officejet;pgid=A8dg.jysql5SRpa2Enk0wm240000oXVzTTnj;sid=InmjFB9cFguqFExk91z0lMZTpfXp1ZjQaLjoMlUDpfXp1YmEFVnJQVGM",
    "http://shopping.hp.com/en_US/home-office/-/products/Ink_Toner_Paper/HP%20Paper;pgid=Hftw5H9FSPtSRpjf9E9rsAgd0000t9zON0KN;sid=VqeX7ybPvLAR73Xt9baJYv7A0SvdLhZgajNRCGqy0SvdLkZfEW6XZ2M1",
    "http://shopping.hp.com/laptops%20&%20hybrids/$800+-+$999",
    "http://shopping.hp.com/en_US/home-office/-/search-SimpleOfferSearch?PageSize=15&SearchTerm=HP+eprint&cattitle=HP%20ePrint",
    "http://shopping.hp.com/desktops%20&%20all-in-ones/high+performance",
    "http://shopping.hp.com/printers/environmental",
    "http://shopping.hp.com/laptops%20&%20hybrids/windows+7",
    "http://shopping.hp.com/en_US/home-office/-/products/Ink_Toner_Paper/HP%20Toner;pgid=Bb5waLgKrp9SRpN9sopouKXe0000qZE9LK2i;sid=NVT2Sd8I5Az3SY2YQVulyQYHBfvK3e-nCcAwrpN1sti8iL-Ycp1ZPZme",
    "http://shopping.hp.com/desktops%20&%20all-in-ones/$1000+-+$1199",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Cases%20and%20Bags",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Cases%20and%20Bags;pgid=hG1QbPXdNm5SRpB38hTLuiui0000n5J-t4pM;sid=pT1rLrFkMBt7LuJHgmQ9rmhrlZJXuoHLmak6VuwwIrEh79H04vT1NNun",
    "http://shopping.hp.com/desktops%20&%20all-in-ones/all-in-one",
    "http://shopping.hp.com/desktops%20&%20all-in-ones/$200+-+$399",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Networking;pgid=hG1QbPXdNm5SRpB38hTLuiui0000ia13l8J6;sid=6cjpYDL3DIf4YGHUi3Dy7er4bkSjobV7owkvh36KbkSjoaQv3uglQneT",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Networking;pgid=hG1QbPXdNm5SRpB38hTLuiui00006eDr43IJ;sid=KZMAxBE3Sb0AxEIUB0dURMg4rh9KBZa7Y1L3y0fXrh9KBYfvHrO2oEFr",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Networking;pgid=hG1QbPXdNm5SRpB38hTLuiui0000hp_7xJT0;sid=QRnI-xvwURCB-0jTcxfUdsP_cbb0bytffY2Zg0akxpWCOntgBtB-n0us",
    "http://shopping.hp.com/hp%20ink/high+capacity+ink+cartridges",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Cases%20and%20Bags;pgid=X59woxn..FlSRpJh43fHDktP0000PJe6U22y;sid=HRt7pOLIvxRhpLG1prApJDvHLbRHMNJnIY-Mq7QompcxZXQQKjtyIZki",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Cases%20and%20Bags;pgid=Hftw5H9FSPtSRpjf9E9rsAgd0000nOrs9XkS;sid=OjGz8o4NXiyg8t0v_DDnclcCCp6PZr6iBqXiitNZvb35MxjVDREVDcBD",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Cases%20and%20Bags;pgid=YJ9wP4gjsrFSRpolOVXirIaA0000GrI0TMR9;sid=iGGgL_mYjgm7L6sLiHPyryCXD-3q7n4UwqBmyLXlD-3q7pkIz6iRT6b_",
    "http://shopping.hp.com/desktops%20&%20all-in-ones/essential+home",
    "http://shopping.hp.com/en_US/home-office/-/products/Ink_Toner_Paper/HP%20Ink;pgid=Cb9gJGxbPONSRpX.PKEA2f1i00003DG3uZEq;sid=tW-f2TiSvVWK2WurEevNWeGdhcCjTQg9ifvOoWXGMuPVGK5Kgk8IzmxB",
    "http://shopping.hp.com/en_US/home-office/-/products/Ink_Toner_Paper/HP%20Ink;pgid=X59woxn..FlSRpJh43fHDktP0000uxjaZpsN;sid=J06OLKlFeTGVLPo4uiKQoXFKoMLE7ZnqG9pIy-U4oMLE7cnVYIfem5fi",
    "http://shopping.hp.com/en_US/home-office/-/products/Ink_Toner_Paper/Brochure%20and%20Presentation%20paper;pgid=Hftw5H9FSPtSRpjf9E9rsAgd00006b06WVJV;sid=jMM_sAfDSNQKsFThuCZoMN7MC091cYBPxgLjCVy1C091cS8186Y_OEI5",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Paper%20Trays;pgid=hG1QbPXdNm5SRpB38hTLuiui00002If_O8wY;sid=sbed_GoIKcqR_Dkrmg6BcbIHgRihaFqnjSNwrSvjNjvXPUL-ztKddC_y",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Paper%20Trays;pgid=X59woxn..FlSRpJh43fHDktP0000IRc6SA7y;sid=z_JaoAGU2cpRoFLpCuYNINib_11mNIYYhTOGGVriSH4QYWEEiDuWgkTw",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Paper%20Trays;pgid=Hftw5H9FSPtSRpjf9E9rsAgd0000_FycsSiL;sid=JPPnir3Ji5ziiu7r4jL7B2XGFFzbHo1mGGcQhespo3-tSzNIUCLW6uKu",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Projectors;pgid=Hftw5H9FSPtSRpjf9E9rsAgd00008bKDqlMq;sid=xgHYlN1sd2brlI5OgJmLFARjQY2SVe3D-pU1xZyHQY2SVUu08SHYHJiW",
    "http://shopping.hp.com/en_US/home-office/-/products/Printers/HP_Officejet_Enterprise",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Other%20Products;pgid=Hftw5H9FSPtSRpjf9E9rsAgd000000Jbwo3t;sid=FvS31Y5XHc-11d12k_3iVVdYJluLQb74KmBahM-8kXj9FADWYiXdBvZP",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Fuser%20and%20Maintenance%20Kits;pgid=Hftw5H9FSPtSRpjf9E9rsAgd0000vdoiX1aT;sid=51PDgbYaWQMxgeU4JZuUAW8V1_z_FTGWrZIu0PfxYN-JQNaKoJrDCfPg",
    "http://shopping.hp.com/laptops%20&%20hybrids/hybrid",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Fuser%20and%20Maintenance%20Kits;pgid=hG1QbPXdNm5SRpB38hTLuiui0000qQwAFCVF;sid=gXYe7t79ikQU7o3erCVIbgfyBvpUL1lxy7fp4YgdBvpUL0gltlaA9LQ-",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Cables;pgid=hG1QbPXdNm5SRpB38hTLuiui00002If_O8wY;sid=sbed_GoIKcqR_Dkrmg6BcbIHgRihaFqnjSNwrSvjNjvXPUL-ztKddC_y",
    "http://shopping.hp.com/printers/11+x+17+in+(a3)",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Tablet%20specialty%20products;pgid=Hftw5H9FSPtSRpjf9E9rsAgd00008hLzY7aJ;sid=CG5V7QcsxAdW7VQOQOpJYN8jOMFpeTeDNPqi4lHMj-IfLC_adws_uEn8",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Tablet%20specialty%20products;pgid=Hftw5H9FSPtSRpjf9E9rsAgd0000x4xo81vM;sid=cYT-UchETKnnUZtmuVKo0RFLQSvCxfjrTRCvKZUQ9gi0kF6cRqTfqokx",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Tablet%20specialty%20products;pgid=hG1QbPXdNm5SRpB38hTLuiui00007GRDNn8z;sid=ElR_PTalw2NvPWWGI1Upve-qIvtDqbEpWJWIMmBFldg1_B5TbTEviggC",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Speakers%20and%20Headsets;pgid=vzFQh34DjJNSRpfVpshk3QwV0000xrHObDpC;sid=ptWjfRVeM7GifUYgpAL2_cxRlnqf6SXxmkFUckO-IVnpvJvf0gSblkLh",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Speakers%20and%20Headsets;pgid=Hftw5H9FSPtSRpjf9E9rsAgd0000NgwhBo8w;sid=aZhW40hyXrRA4xtQpFECY5F9WTdqd8_-I1mh7B6S7hQcImCEFv3wHAY8",
    "http://shopping.hp.com/tablets/7+inches",
    "http://shopping.hp.com/desktops%20&%20all-in-ones/gaming",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Accessories?SearchParameter",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Storage;pgid=Hftw5H9FSPtSRpjf9E9rsAgd00004ze5n2J4;sid=fB-iQ38k9yieQywGzqa8zqcr-5Pogk-LQItVTCnE-5Pogh-0O9aaLh5T",
    "http://shopping.hp.com/en_US/home-office/-/products/Printers/HP%20Scanjet%20and%20HP%20Fax;pgid=gj1Aupo5AtdSRpy_p5gtn3WU0000vXczfTgf;sid=-IGw3tBBKdC13oI9G6esUwhOyC6MSuDuxBXhpo0Vfw36H0aZz6Hgae7m",
    "http://shopping.hp.com/en_US/home-office/-/products/Ink_Toner_Paper/Premium%20Plus%20photo%20paper",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Tablet%20cases%20and%20sleeves;pgid=Hftw5H9FSPtSRpjf9E9rsAgd00004ze5n2J4;sid=fB-iQ38k9yieQywGzqa8zqcr-5Pogk-LQItVTCnE-5Pogh-0O9aaLh5T",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Tablet%20cases%20and%20sleeves;pgid=hG1QbPXdNm5SRpB38hTLuiui0000zCHst_Dy;sid=mTVL2U2tG1xH2R6OoKxQVJWiqZp3Tcoh0_SNPgHQHrkBGGVb5lBA3VR1",
    "http://shopping.hp.com/desktops%20&%20all-in-ones/windows+8",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Mice%20and%20Keyboards;pgid=hG1QbPXdNm5SRpB38hTLuiui0000ia13l8J6;sid=6cjpYDL3DIf4YGHUi3Dy7er4bkSjobV7owkvh36KbkSjoaQv3uglQneT",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Mice%20and%20Keyboards?SearchParameter",
    "http://shopping.hp.com/desktops%20&%20all-in-ones/$1200+and+up",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Storage;pgid=hG1QbPXdNm5SRpB38hTLuiui0000zCHst_Dy;sid=mTVL2U2tG1xH2R6OoKxQVJWiqZp3Tcoh0_SNPgHQHrkBGGVb5lBA3VR1",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Storage;pgid=Hftw5H9FSPtSRpjf9E9rsAgd0000jdCTfJPz;sid=fE-xn84pJw3pn50Lww7jHxcmTOCNC_6GQNvg55N9-8P7XubfAyqJ8q9e",
    "http://shopping.hp.com/en_US/home-office/-/products/Ink_Toner_Paper/Printheads;pgid=YJ9wP4gjsrFSRpolOVXirIaA0000Kj6c-1k_;sid=m8VNTT7CrttITWxRrwBQwObNHEkHjA5tp1GLqnK_HEkHjKgarOWBb3um",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Mice%20and%20Keyboards;pgid=hG1QbPXdNm5SRpB38hTLuiui00002If_O8wY;sid=sbed_GoIKcqR_Dkrmg6BcbIHgRihaFqnjSNwrSvjNjvXPUL-ztKddC_y",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Mice%20and%20Keyboards;pgid=Cb9gJGxbPONSRpX.PKEA2f1i0000emf9YquV;sid=csjifLzC_o_pfO_73k-w_GXN9USovYxtTlwVc-oi9USovSoaRehUGOye",
    "http://shopping.hp.com/software/graphics/multimedia",
    "http://shopping.hp.com/software/total+training",
    "http://shopping.hp.com/laptops%20&%20hybrids/flyer+red",
    "http://shopping.hp.com/laptops%20&%20hybrids/pearl+white",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Docking%20Stations%20and%20Stands;pgid=Hftw5H9FSPtSRpjf9E9rsAgd0000C1dL49yz;sid=x9k-KR70h-gMKU3WhINrqcf793YCvZl4jRjTeF8fQFV06H5kgBBU-mbs",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Docking%20Stations%20and%20Stands;pgid=Hftw5H9FSPtSRpjf9E9rsAgd0000_FycsSiL;sid=JPPnir3Ji5ziiu7r4jL7B2XGFFzbHo1mGGcQhespo3-tSzNIUCLW6uKu",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Privacy_Screens;pgid=Hftw5H9FSPtSRpjf9E9rsAgd0000GuAD3aPj;sid=_zOsINAQnhuwIIMyOg7-oAkfz5yQtOC_w6dbL4bweL_m4UbIyBNa4OUD",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Privacy_Screens;pgid=Hftw5H9FSPtSRpjf9E9rsAgd0000NgwhBo8w;sid=aZhW40hyXrRA4xtQpFECY5F9WTdqd8_-I1mh7B6S7hQcImCEFv3wHAY8",
    "http://shopping.hp.com/en_US/home-office/-/products/Ink_Toner_Paper/Advanced%20photo%20paper;pgid=X59woxn..FlSRpJh43fHDktP000053k9nQB6;sid=mWaJuWpfQEuouTkiKVKSNLJQqcm1LVrwpfLYwTcLHurDePyHrkZFmy87",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Monitor_Mounts_and_Stands;pgid=hG1QbPXdNm5SRpB38hTLuiui0000qQwAFCVF;sid=gXYe7t79ikQU7o3erCVIbgfyBvpUL1lxy7fp4YgdBvpUL0gltlaA9LQ-",
    "http://shopping.hp.com/en_US/home-office/-/products/Care_Packs/Care%20Packs%20for%20Tablets;pgid=Hftw5H9FSPtSRpjf9E9rsAgd0000C1dL49yz;sid=x9k-KR70h-gMKU3WhINrqcf793YCvZl4jRjTeF8fQFV06H5kgBBU-mbs",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Tablet%20cases%20and%20sleeves;pgid=Hftw5H9FSPtSRpjf9E9rsAgd0000nOrs9XkS;sid=OjGz8o4NXiyg8t0v_DDnclcCCp6PZr6iBqXiitNZvb35MxjVDREVDcBD",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Tablet%20cases%20and%20sleeves;pgid=Hftw5H9FSPtSRpjf9E9rsAgd0000s0nptNjh;sid=B2loE_nWs1pOE6r0Q7o9kyDZN8ZUh8l5O_05a6SCgOUi0plGQKDed6mK",
    "http://shopping.hp.com/en_US/home-office/-/products/Printers/HP%20Designjet;pgid=X59woxn..FlSRpJh43fHDktP0000uxjaZpsN;sid=J06OLKlFeTGVLPo4uiKQoXFKoMLE7ZnqG9pIy-U4oMLE7cnVYIfem5fi",
    "http://shopping.hp.com/laptops%20&%20hybrids/under+12+inches",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Printer_Networking;pgid=hG1QbPXdNm5SRpB38hTLuiui0000hp_7xJT0;sid=QRnI-xvwURCB-0jTcxfUdsP_cbb0bytffY2Zg0akxpWCOntgBtB-n0us",
    "http://shopping.hp.com/en_US/home-office/-/products/Printers/HP%20ENVY",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/CD%20DVD%20and%20Blu-Ray;pgid=Hftw5H9FSPtSRpjf9E9rsAgd0000xXMrZGim;sid=EYdBGr5goqlYGu1CqeUVmmdvISh9jo7PLROsS_-LlgsL2zDhZVbWDeqz",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/CD%20DVD%20and%20Blu-Ray;pgid=hG1QbPXdNm5SRpB38hTLuiui0000qk82Zwdk;sid=SB1uYFYdF3xoYAU-a4x17Y4Sz5EkoWaydImZbwD9z5EkoTaND9STLD8s",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/CD%20DVD%20and%20Blu-Ray;pgid=Hftw5H9FSPtSRpjf9E9rsAgd000000Jbwo3t;sid=FvS31Y5XHc-11d12k_3iVVdYJluLQb74KmBahM-8kXj9FADWYiXdBvZP",
    "http://shopping.hp.com/laptops%20&%20hybrids/regal+purple",
    "http://shopping.hp.com/en_US/home-office/-/products/Laptops/HP%20Split",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Surge%20Protection%20and%20Power%20Supplies;pgid=Hftw5H9FSPtSRpjf9E9rsAgd0000s0nptNjh;sid=B2loE_nWs1pOE6r0Q7o9kyDZN8ZUh8l5O_05a6SCgOUi0plGQKDed6mK",
    "http://shopping.hp.com/software/adobe",
    "http://shopping.hp.com/en_US/home-office/-/products/Printers/HP%20Photosmart",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Webcams;pgid=gj1Aupo5AtdSRpy_p5gtn3WU0000vXczfTgf;sid=-IGw3tBBKdC13oI9G6esUwhOyC6MSuDuxBXhpo0Vfw36H0aZz6Hgae7m",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Calculators",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Batteries;pgid=Hftw5H9FSPtSRpjf9E9rsAgd0000Omt55KJ5;sid=t-FH-KG5xZAC-PKbf5RbdXm2h057bJEWi3Ww9_dZMG0NOcEp8ChHcORD",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Batteries;pgid=Hftw5H9FSPtSRpjf9E9rsAgd0000SD3IIHBg;sid=3kKeLPPCczbQLKDgFGWCoSvN7u2iuMNt4tZYy7-_Wc7U7ZNSmYvFF5FH",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Monitor_Mounts_and_Stands;pgid=Hftw5H9FSPtSRpjf9E9rsAgd0000LdFpYSKd;sid=3UhzxdxTPGdvxY9x5RxuSARc7edPUez84dyEyoqzWsQ5BLzDmoFj1oe7",
    "http://shopping.hp.com/en_US/home-office/-/products/Desktops/HP%20110",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/AC%20Adapters;pgid=Hftw5H9FSPtSRpjf9E9rsAgd00008bKDqlMq;sid=xgHYlN1sd2brlI5OgJmLFARjQY2SVe3D-pU1xZyHQY2SVUu08SHYHJiW",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/AC%20Adapters;pgid=hG1QbPXdNm5SRpB38hTLuiui0000n5J-t4pM;sid=pT1rLrFkMBt7LuJHgmQ9rmhrlZJXuoHLmak6VuwwIrEh79H04vT1NNun",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/AC%20Adapters;pgid=EdNQvc4lM1hSRpWR7DDy74uK0000ZuC0iP5f;sid=gv8WE4WKZ5sTE9cbCGhDk1yFslAqhwIGyD7Kqt78BXNc0uUaxTbrX-y7",
    "http://shopping.hp.com/en_US/home-office/-/products/Ink_Toner_Paper/Everyday%20photo%20paper",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Printer%20Cables;pgid=Hftw5H9FSPtSRpjf9E9rsAgd0000_FycsSiL;sid=JPPnir3Ji5ziiu7r4jL7B2XGFFzbHo1mGGcQhespo3-tSzNIUCLW6uKu",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Webcams;pgid=hG1QbPXdNm5SRpB38hTLuiui0000ia13l8J6;sid=6cjpYDL3DIf4YGHUi3Dy7er4bkSjobV7owkvh36KbkSjoaQv3uglQneT",
    "http://shopping.hp.com/en_US/home-office/-/products/Accessories/Webcams;pgid=Hftw5H9FSPtSRpjf9E9rsAgd0000dNZn1jW-;sid=CQXgBT84-FTyBWwZgLi0heY3jomqxLi0Q8QXCmnYjomqxF-oTswWxQor",
    "http://shopping.hp.com/hp%20ink/super-combo+packs",
    "http://shopping.hp.com/monitors/under+28+inches",
    "http://shopping.hp.com/software/cyberlink",
    "http://shopping.hp.com/en_US/home-office/-/products/Ink_Toner_Paper/Photo%20Value%20Packs;pgid=X59woxn..FlSRpJh43fHDktP0000uxjaZpsN;sid=J06OLKlFeTGVLPo4uiKQoXFKoMLE7ZnqG9pIy-U4oMLE7cnVYIfem5fi",
    "http://shopping.hp.com/software/intuit",
    "http://shopping.hp.com/en_US/home-office/-/products/Tablets/HP",
    "http://shopping.hp.com/en_US/home-office/-/products/Ink_Toner_Paper/Premium%20photo%20paper",
    "http://shopping.hp.com/webcams/logitech",
    "http://shopping.hp.com/laptops%20&%20hybrids/ultrabooks",
    "http://shopping.hp.com/calculators/science",
    "http://shopping.hp.com/monitors/$250+-+$299",
    "http://shopping.hp.com/laptops%20&%20hybrids/android",
    "http://shopping.hp.com/software/management",
    "http://shopping.hp.com/laptops%20&%20hybrids/windows+8",
    "http://shopping.hp.com/monitors/$200+-+$249",
    "http://www.build.com.*/s[0-9]+",
    ".*\n.*",
    "http://www.moosejaw.com/moosejaw/shop/product_Thule-Traverse-Fit-Kit_10208069_10208_10000001_-1_.*",
    "http://www.rei.com/product/799940/thule-traverse-fit-kit.*"
  )

  override def getLines(file: String) = {
    file match {
      case "whitelist" => defaultWhiteList
      case "blacklist" => defaultBlackList
      case _ => Iterator("%d".format(new DateTime(1970,1,1,0,0).getMillis))
    }
  }

  override def write(file: String, content: Iterator[String]) = {
  }
}

trait FSTestThatReturnsRecentSnapShotDate extends FSTest {
  def snapshotDate = new DateTime(2013, 6, 10, 0, 0) // 10th june 2013 was the last snapshot
  override def exists(file: String, hdfsHost: String = "") = true
  override def getLines(file: String) = {
    file match {
      case "whitelist" => defaultWhiteList
      case "blacklist" => defaultBlackList
      case _ => Iterator("%d".format(snapshotDate.getMillis))
    }
  }
}

trait FSTestThatReturnsYesterdayAsSnapShotDate extends FSTest {
  def snapshotDate = new DateTime().minusDays(1)
  override def exists(file: String, hdfsHost: String = "") = true
  override def getLines(file: String) = {
    file match {
      case "whitelist" => defaultWhiteList
      case "blacklist" => defaultBlackList
      case _ => Iterator("%d".format(snapshotDate.getMillis))
    }
  }
}
