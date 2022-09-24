import type { NextPage } from "next";
import Image from "next/image";

import { NavigationLinks } from "../components/Navigation";

const Home: NextPage = () => {
  return (
    <div className="flex flex-col items-center justify-center gap-20 h-full w-full">
      <Image layout="fill" sizes="33vw" src={require('../public/sand.png')} objectFit="cover" placeholder="blur" />
      <div className="relative max-w-[400px] w-[90%] h-[300px] md:h-[400px]">
        <Image layout="fill" src='/alchemy.svg' />
      </div>
      <div className="flex flex-row gap-10 border-blue border-1">
        <a target="_blank" rel="noopener noreferrer" href={NavigationLinks.discord}>
          <Image className="transition duration-300 hover:opacity-40 cursor-pointer" src="/discord.svg" width={40} height={40} />
        </a>
        <a target="_blank" rel="noopener noreferrer" href={NavigationLinks.twitter}>
          <Image className="transition duration-300 hover:opacity-40 cursor-pointer" src="/twitter.svg" width={40} height={40} />
        </a>
        <a target="_blank" rel="noopener noreferrer" href={NavigationLinks.docs}>
          <Image className="transition duration-300 hover:opacity-40 cursor-pointer" src="/docs.svg" width={40} height={35} />
        </a>
      </div>
    </div>
  );
};

export default Home;
