import { DefaultSeoProps } from "next-seo";

const APP_DESCRIPTION = "Blazing fast decentralized exchange on the Aptos blockchain!"

const APP_DEFAULT_SEO: DefaultSeoProps = {
  title: "Ferum",
  titleTemplate: "%s",
  description: APP_DESCRIPTION,
  canonical: "https://www.ferum.xyz",
  additionalLinkTags: [
    {
      rel: "icon",
      href: "/favicon.ico",
    },
    {
      rel: "preload",
      href: "/fonts/euclid-circular-a-regular.woff2",
      as: "font",
      type: "font/woff2",
      crossOrigin: "anonymous",
    },
  ],
  openGraph: {
    type: "website",
    locale: "en_US",
    url: "ferum.xyz",
    title: "ferum",
    description: APP_DESCRIPTION,
    images: [
      {
        url: "https://www.ferum.xyz/assets/og-cover-photo.png",
        width: 2000,
        height: 1000,
        alt: "Ferum Cover Photo",
        type: "image/jpeg",
        secureUrl: "https://www.ferum.xyz/assets/og-cover-photo.png",
      },
    ],
    site_name: "ferum.xyz",
  },
  twitter: {
    handle: "@ferumxyz",
    site: "ferum.xyz",
    cardType: "summary_large_image",
  },
};

export default APP_DEFAULT_SEO;